from __future__ import annotations

import os
from typing import Any, ClassVar

from eventsourcing.persistence import InfrastructureFactory
from eventsourcing.utils import Environment, EnvType


class DCBApplication:
    name = "DCBApplication"
    env: ClassVar[dict[str, str]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if "name" not in cls.__dict__:
            cls.name = cls.__name__

    def __init__(self, env: EnvType | None = None) -> None:
        self.env = self.construct_env(self.name, env)  # type: ignore[misc]
        self.factory = InfrastructureFactory.construct(self.env)
        self.events = self.factory.dcb_event_store()
        self.transcoder = self.factory.transcoder()

    def construct_env(self, name: str, env: EnvType | None = None) -> Environment:
        """Constructs environment from which application will be configured."""
        _env = dict(type(self).env)
        _env.update(os.environ)
        if env is not None:
            _env.update(env)
        return Environment(name, _env)
