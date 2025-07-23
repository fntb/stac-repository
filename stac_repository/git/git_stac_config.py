from typing import (
    Optional
)

from pydantic import (
    BaseModel,
    Field
)


class GitStacConfig(BaseModel):
    repository: str

    use_lfs: Optional[str] = Field(
        default=None,
        description="URL of the GitLFS server, if any"
    )
