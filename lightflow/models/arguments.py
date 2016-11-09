
class Option:
    def __init__(self, name, default=None, help=None, type=None):
        self._name = name
        self._default = default
        self._help = help
        self._type = type

    @property
    def name(self):
        return self._name

    @property
    def default(self):
        return self._default

    def convert(self, value):
        if self._type is str:
            return str(value)
        elif self._type is int:
            return int(value)
        elif self._type is float:
            return float(value)
        elif self._type is bool:
            return bool(value)
        else:
            return value


class Arguments(list):
    def __init__(self, *args):
        super().__init__(*args)

    def check_missing(self, args):
        return [opt.name for opt in self
                if (opt.name not in args) and (opt.default is None)]

    def consolidate(self, args):
        result = dict(args)

        for opt in self:
            if opt.name in result:
                result[opt.name] = opt.convert(result[opt.name])
            else:
                if opt.default is not None:
                    result[opt.name] = opt.default

        return result
