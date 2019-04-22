import re

class Param:

    def __init__(self, model, level, param, time=None, georange=None,
            name=None, unit=None, key=None, purekey=None):
        self.model = model
        self.level = level
        self.param = param
        self.time = time
        self.georange = georange
        self.name = name
        self.unit = unit
        self.data = None
        self.key = key
        self.purekey = purekey

    def __hash__(self):
        return hash(self.purekey)

    def __str__(self):
        return self.key

    def __repr__(self):
        return self.key

    def process(self):
        pass

    @classmethod
    def from_str(cls, str_, model=None):
        """A common instance of param string is {level}:{param}[/{time}].
        There is no special code for level and param naming as model params
        are various, but we recommend you to follow certain rules, e.g. `500`
        for 500hPa level, `surface` for surface level, `10m` for 10m above
        ground. They are both case insensitive.

        A typical param: `500:z`, `850:t`, `850:u/-24` ...
        """
        regex = r"([a-z0-9]+):([a-z0-9]+)(\/([-0-9]+))*"
        str_ = str_.lower()
        matches = re.match(regex, str_)
        purekey = str_.split('/')[0]
        ins = cls(model, matches.group(1), matches.group(2),
            time=matches.group(4), key=str_, purekey=purekey)
        return ins

    @classmethod
    def to_purekey(cls, str_):
        return str_.split('/')[0]
