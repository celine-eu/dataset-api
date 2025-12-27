import logging
from typing import Any, Dict, Optional, List
from dataclasses import asdict, dataclass
import httpx

from celine.dataset.api.catalogue.models import DatasetEntry
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetOPAInput:
    id: str
    access_level: str


@dataclass(frozen=True)
class UserOPAInput:
    sub: str
    roles: List[str]


@dataclass(frozen=True)
class OPAInput:
    action: str
    resource: str
    dataset: DatasetOPAInput
    user: UserOPAInput | None


def _get_opa_payload(input_obj: OPAInput) -> Dict[str, Any]:
    return {"input": asdict(input_obj)}


def _build_opa_input(
    *,
    dataset: DatasetEntry,
    user: AuthenticatedUser | None,
) -> OPAInput:
    return OPAInput(
        action="read",
        resource="dataset",
        dataset=DatasetOPAInput(
            id=dataset.dataset_id,
            access_level=dataset.access_level or "restricted",
        ),
        user=(
            UserOPAInput(
                sub=user.sub,
                roles=user.roles,
            )
            if user
            else None
        ),
    )


class OPAClient:
    def __init__(self, base_url: str, policy_path: str):
        self._url = f"{base_url.rstrip('/')}/v1/data/{policy_path.lstrip('/')}"
        logger.debug(f"OPA URL {self._url}")

    async def evaluate(
        self, dataset: DatasetEntry, user: AuthenticatedUser | None
    ) -> bool:
        payload = _get_opa_payload(_build_opa_input(dataset=dataset, user=user))

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(self._url, json=payload)

            try:
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"OPA request failed {e}")
                raise Exception

            data = resp.json()

        result = data.get("result")
        if not isinstance(result, bool):
            logger.warning(f"OPA response format error, missing 'result'")
            raise RuntimeError("OPA policy returned invalid result")

        return result
