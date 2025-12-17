import logging
from typing import Any, Dict
import httpx

logger = logging.getLogger(__name__)


class OPAClient:
    def __init__(self, base_url: str, policy_path: str):
        self._url = f"{base_url.rstrip('/')}/v1/data/{policy_path.lstrip('/')}"

    async def evaluate(self, input_doc: Dict[str, Any]) -> bool:
        payload = {"input": input_doc}

        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(self._url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        result = data.get("result")
        if not isinstance(result, bool):
            raise RuntimeError("OPA policy returned invalid result")

        return result
