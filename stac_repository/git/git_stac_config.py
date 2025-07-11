from typing import (
    Optional
)

from pydantic import (
    BaseModel,
    Field
)


class GitStacConfig(BaseModel):
    git_lfs_url: Optional[str] = Field(
        default=None,
        description="Git LFS Backend to use"
    )
