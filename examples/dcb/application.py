from __future__ import annotations

import os
from typing import Any, ClassVar

from eventsourcing.utils import Environment, EnvType
from examples.dcb.api import DCBInfrastructureFactory


class DCBApplication:
    name = "DCBApplication"
    env: ClassVar[dict[str, str]] = {}

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
