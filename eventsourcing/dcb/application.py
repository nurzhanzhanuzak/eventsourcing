from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, ClassVar

from eventsourcing.dcb.persistence import DCBInfrastructureFactory
from eventsourcing.utils import Environment, EnvType

if TYPE_CHECKING:
    from typing_extensions import Self


class DCBApplication:
    name = "DCBApplication"
    env: ClassVar[dict[str, str]] = {"PERSISTENCE_MODULE": "eventsourcing.dcb.popo"}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    def __init__(self, env: EnvType | None = None):
        self.env = self.construct_env(self.name, env)  # type: ignore[misc]
        self.factory = DCBInfrastructureFactory.construct(self.env)
        self.recorder = self.factory.dcb_event_store()

    def construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """Constructs environment from which application will be configured."""
        _env = dict(type(self).env)
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self.factory.close()
