"""DPS-06 — the pull token keeps EDC's shape and only verifies with our key."""
from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from celine.dataset.dps.tokens import InvalidToken, TokenIssuer


def _pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def test_claims_follow_edc_data_plane_iam() -> None:
    issuer = TokenIssuer(_pem(), key_id="k1")
    token, jti = issuer.issue(issuer="did:web:p", audience="did:web:c")

    header = jwt.get_unverified_header(token)
    assert header["alg"] == "ES256" and header["kid"] == "k1"

    claims = issuer.verify(token)
    assert claims["jti"] == jti
    assert claims["iss"] == claims["sub"] == "did:web:p"
    assert claims["aud"] == "did:web:c"
    assert abs(claims["iat"] - time.time()) < 5
    assert "exp" not in claims
    # The flow facts stay server-side.
    assert not {"agreement_id", "asset_id", "process_id"} & set(claims)


def test_a_configured_key_survives_a_new_issuer() -> None:
    pem = _pem()
    token, _ = TokenIssuer(pem).issue(issuer="p", audience="c")
    assert TokenIssuer(pem).verify(token)["aud"] == "c"


def test_another_key_is_refused() -> None:
    token, _ = TokenIssuer(ephemeral=True).issue(issuer="p", audience="c")
    with pytest.raises(InvalidToken):
        TokenIssuer(ephemeral=True).verify(token)


def test_sub_must_equal_iss() -> None:
    pem = _pem()
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    forged = jwt.encode({"jti": "j", "iss": "p", "sub": "x", "aud": "c"}, key, algorithm="ES256")
    with pytest.raises(InvalidToken):
        TokenIssuer(pem).verify(forged)


def test_a_missing_jti_is_refused() -> None:
    pem = _pem()
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    token = jwt.encode({"iss": "p", "sub": "p", "aud": "c"}, key, algorithm="ES256")
    with pytest.raises(InvalidToken):
        TokenIssuer(pem).verify(token)


def test_a_non_ec_key_is_rejected_at_startup() -> None:
    from cryptography.hazmat.primitives.asymmetric import rsa

    rsa_pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    with pytest.raises(ValueError):
        TokenIssuer(rsa_pem)


def test_without_a_key_nothing_is_signed() -> None:
    """Workers must share the key, and it must survive a restart (DPS-06)."""
    with pytest.raises(ValueError, match="DPS_TOKEN_SIGNING_KEY"):
        TokenIssuer()
