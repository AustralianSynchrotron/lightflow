

class Graph:
    def __init__(self, name: str, *, autostart: bool = True, schema: dict = None):
        self._name = name
        self._autostart = autostart
        self._schema = schema

    @property
    def name(self):
        return self._name

    def define(self, schema: dict, *, validate: bool = True):
        if validate:
            self._validate(schema)

        self._schema = schema

    def _validate(self, schema: dict):
        pass
