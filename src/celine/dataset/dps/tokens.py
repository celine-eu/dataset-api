"""The pull token, shaped like EDC's own (DPS-06).

EDC's `DataPlaneAuthorizationServiceImpl` mints `jti`, `aud` = the counterparty,
`iss` = `sub` = the provider, `iat`; its `DefaultDataPlaneAccessTokenServiceImpl`
keeps the flow facts server-side under the `jti` and validates `sub == iss` and
a present `jti`. Same shape here, so the legacy PEP's notion of "the consumer is
`aud`" carries over unchanged. No `exp`: the flow state is the revocation, and
EDC 0.18.0 has no refresh either.
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Optional

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

ALGORITHM = "ES256"


class InvalidToken(Exception):
    pass


class TokenIssuer:
    """Signs and verifies pull tokens.

    **The key is configuration, not generated.** Every worker must verify what
    any worker signed, and a token must outlive a restart, just as its flow
    does. `ephemeral=True` exists for tests only.
    """

    def __init__(
        self,
        private_key_pem: Optional[str] = None,
        key_id: Optional[str] = None,
        *,
        ephemeral: bool = False,
    ):
        if private_key_pem:
            key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
            if not isinstance(key, ec.EllipticCurvePrivateKey):
                raise ValueError("the DPS token key must be an EC private key")
        elif ephemeral:
            key = ec.generate_private_key(ec.SECP256R1())
        else:
            raise ValueError(
                "DPS_TOKEN_SIGNING_KEY is required: without a shared key, workers "
                "cannot verify each other's pull tokens and a restart voids them all"
            )
        self._private_key = key
        self._public_key = key.public_key()
        self._key_id = key_id

    def issue(self, *, issuer: str, audience: str) -> tuple[str, str]:
        """A signed token and its `jti`."""
        token_id = str(uuid.uuid4())
        claims = {
            "jti": token_id,
            "iss": issuer,
            "sub": issuer,
            "aud": audience,
            "iat": int(time.time()),
        }
        headers = {"kid": self._key_id} if self._key_id else None
        token = jwt.encode(claims, self._private_key, algorithm=ALGORITHM, headers=headers)
        return token, token_id

    def verify(self, token: str) -> dict[str, Any]:
        try:
            claims = jwt.decode(
                token,
                self._public_key,
                algorithms=[ALGORITHM],
                options={
                    "verify_aud": False,  # `aud` is the answer, not a check
                    "require": ["jti", "iss", "sub", "aud"],
                },
            )
        except jwt.PyJWTError as exc:
            raise InvalidToken(str(exc)) from exc
        if claims["sub"] != claims["iss"]:
            raise InvalidToken("'sub' and 'iss' must be equal")
        audience = claims["aud"]
        if isinstance(audience, list):
            if len(audience) != 1:
                raise InvalidToken("exactly one audience expected")
            claims["aud"] = audience[0]
        return claims
