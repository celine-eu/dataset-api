import logging
from typing import Any, Dict
import httpx

logger = logging.getLogger(__name__)


class OPAClient:
    def __init__(self, base_url: str, policy_path: str):
        self._url = f"{base_url.rstrip('/')}/v1/data/{policy_path.lstrip('/')}"
        logger.debug(f"OPA URL {self._url}")

    async def evaluate(self, input_doc: Dict[str, Any]) -> bool:
        payload = {"input": input_doc}

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
