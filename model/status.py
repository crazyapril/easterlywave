import datetime

from django.core.cache import cache

from model.models import PlotModel
from model.kicker import models_runtime
from tools.cache import Key


def register_plot_model():
    from model.registry import registry_center
    PlotModel.objects.all().delete()
    for task in registry_center.all_tasks:
        PlotModel.register(task.model, task.regions, task.category,
            task.name, task.code)
    models = make_model_list()
    Key.set(Key.MODEL_MODELS, models, None)
    regions = make_region_list()
    Key.set(Key.MODEL_REGIONS, regions, None)
    cache.delete_pattern('CODES_*')

def make_model_list():
    model_set = set(PlotModel.objects.all().values_list('model', flat=True))
    model_set = list(model_set)
    model_set.sort()
    return model_set

def make_region_list():
    region_set = set(PlotModel.objects.all().values_list('region', flat=True))
    region_set = list(region_set)
    region_set.sort()
    return region_set

def select_name_and_code(model, region):
    id_key = 'CODES_{}_{}'.format(model, region)
    codes = cache.get(id_key)
    if codes is None:
        codes = list(PlotModel.objects.filter(model=model, region=region).\
            order_by('name').values('code', 'name'))
        cache.set(id_key, codes, 30 * Key.DAY)
    return codes

def get_update_status(model, region, code):
    key = 'STATUS_{}_{}_{}'.format(model, region, code)
    status = cache.get(key) or models_runtime.get(model, [])
    return status
