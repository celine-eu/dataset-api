from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict


class AuthenticatedUser(BaseModel):
    """
    Normalized authenticated identity extracted from a validated JWT.

    This model is:
    - issuer-agnostic
    - policy-engine friendly
    - stable across IdP implementations
    """

    sub: str = Field(..., description="Subject identifier (user id)")
    username: Optional[str] = Field(None, description="Human-readable username")
    email: Optional[str] = None

    roles: List[str] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)
    scopes: List[str] = Field(default_factory=list)

    issuer: Optional[str] = None
    audiences: list[str] = Field(default_factory=list)

    # Keep raw JWT claims for OPA / auditing / future extensions
    claims: Dict[str, Any] = Field(default_factory=dict)

    token: Optional[str] = Field(default=None, exclude=True)

    model_config = ConfigDict(
        frozen=True,
        extra="ignore",
    )
