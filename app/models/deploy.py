from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class SwitchBundleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bundle_id: str
