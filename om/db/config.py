#!coding: utf-8
from threading import RLock
from om.exceptions import ImproperlyConfig


class ConnectionSpec(object):
    def validate(self):
        raise NotImplementedError(u"Implement the method by subclass")


class ConnectionRegistry(object):
    def __init__(self):
        self._lock = RLock()
        self._config_map = dict()

    def __getitem__(self, alias):
        """

        Args:
            alias:
        Returns:
            ConnectionSpec
        """
        self._lock.acquire()
        try:
            if alias not in self._config_map:
                raise ImproperlyConfig(u"connection %s config lost", alias)
            return self._config_map[alias]
        finally:
            self._lock.release()

    def __setitem__(self, alias, spec):
        """
        Args:
            alias(str):
            spec(ConnectionSpec):
        """
        spec.validate()
        self._lock.acquire()
        try:
            self._config_map[alias] = spec
        finally:
            self._lock.release()


connections = ConnectionRegistry()
