import logging
from typing import Any, Dict, Optional, List
from dataclasses import asdict, dataclass
import httpx

from celine.dataset.api.catalogue.models import DatasetEntry
from celine.dataset.security.disclosure import AccessLevel
from celine.dataset.security.models import AuthenticatedUser

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetOPAInput:
    id: str
    access_level: AccessLevel


@dataclass(frozen=True)
class SubjectOPAInput:
    sub: str
    roles: List[str]
    groups: List[str]
    scopes: List[str]


@dataclass(frozen=True)
class OPAInput:
    action: str
    resource: str
    dataset: DatasetOPAInput
    subject: SubjectOPAInput | None


def _get_opa_payload(input_obj: OPAInput) -> Dict[str, Any]:
    return {"input": asdict(input_obj)}


def _build_opa_input(*, dataset: DatasetEntry, user: AuthenticatedUser | None):
    return OPAInput(
        action="read",
        resource="dataset",
        dataset=DatasetOPAInput(
            id=dataset.dataset_id,
            access_level=AccessLevel(dataset.access_level or "restricted"),
        ),
        subject=(
            SubjectOPAInput(
                sub=user.sub,
                roles=user.roles,
                groups=user.groups,
                scopes=user.scopes,
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
    ) -> bool | None:
        payload = _get_opa_payload(_build_opa_input(dataset=dataset, user=user))

        resp = None
        data = {"result": False}
        try:

            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(self._url, json=payload)
                resp.raise_for_status()
                data = resp.json()

        except httpx.HTTPStatusError as e:
            logger.error(
                "OPA HTTP error",
                extra={
                    "status": e.response.status_code,
                    "body": e.response.text,
                },
            )

        except httpx.ReadError as e:
            logger.error(f"OPA read error")

        except httpx.RequestError as e:
            logger.error(f"OPA connection error {e}")

        except ValueError as e:
            logger.error(
                "OPA returned invalid JSON",
                extra={"body": resp.text if resp else None},
            )

        result: Any | None = data.get("result", None)
        if result is None or result.get("allow", None) is None:
            logger.warning(f"OPA response format error, missing 'result': {data}")
            return None

        allow = result.get("allow")
        if not isinstance(allow, bool):
            logger.warning(f"OPA response format error, 'allow' is not bool: {allow}")
            return None

        logger.debug(f"OPA result is {allow} for {payload}")
        return allow
