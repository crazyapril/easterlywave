from django.utils.module_loading import import_string

from model.models import PlotModel
from model.param import Param
from tools.cache import Key
from tools.mapstore import get_area_keys

PRIORITY_HIGH = 1
PRIORITY_MEDIUM = 4
PRIORITY_LOW = 8

MODEL_ECMWF = 'ECMWF'
MODEL_GFS = 'GFS'


class PlotTask:

    def __init__(self, model, params, regions=None, times=None, priority=None,
            plotfunc=None, code=None, georange=None, scope=None, category=None,
            name=None):
        self.model = model
        self.params = params
        self.regions = regions
        self.times = times
        self.priority = priority or PRIORITY_LOW
        self.plotfunc = plotfunc
        self.code = code
        self.georange = georange
        self.scope = scope
        self.category = category
        self.name = name
        self.requested_ticks = []

    def validate(self):
        return False

    def to_json(self):
        json = {
            'model': self.model,
            'params': self.params,
            'regions': self.regions,
            'times': self.times,
            'priority': self.priority,
            'code': self.code,
            'georange': self.georange,
            '_func_path': self.scope + '.' + self.plotfunc.__name__
        }
        return json

    @classmethod
    def from_json(cls, json):
        instance = cls(json['model'], json['params'], regions=json['params'],
            times=json['times'], priority=json['priority'], code=json['code'],
            georange=json['georange'])
        instance.plotfunc = import_string(json['_func_path'])
        return instance


class RegistryCenter:

    registry = {}
    all_tasks = []

    def add_task(self, task):
        if task.model not in self.registry:
            self.registry[task.model] = {}
        for param in task.params:
            purekey = Param.to_purekey(param)
            if purekey not in self.registry[task.model]:
                self.registry[task.model][purekey] = []
            self.registry[task.model][purekey].append(task)
        self.all_tasks.append(task)

    def get_tasks(self, model, paramkey):
        return self.registry[model][paramkey]

    def get_params(self, model):
        return list(self.registry[model].keys())

    def iter_tasks(self, model):
        for paramkey in self.registry[model]:
            tasks = self.registry[model][paramkey]
            for task in tasks:
                yield task

registry_center = RegistryCenter()


def register(model, params, regions=None, times=None, priority=None,
        code=None, category=None, name=None, scope=None):
    def wrapper(plotfunc):
        nonlocal regions, times, priority, code, category, name, scope
        code = code or plotfunc.__name__
        task = PlotTask(model, params, regions=regions, times=times,
            priority=priority, plotfunc=plotfunc, code=code, scope=scope,
            category=category, name=name)
        registry_center.add_task(task)
        #PlotModel.register(model, regions, category, name, code)
        return plotfunc
    return wrapper


import model.tasksets.ecmwf
