from django.db import models

from tools.mapstore import get_area_keys


class PlotModel(models.Model):

    category = models.CharField(max_length=32)
    name = models.CharField(max_length=64)
    code = models.CharField(max_length=16)
    region = models.CharField(max_length=24)
    model = models.CharField(max_length=16)

    @classmethod
    def register(cls, model, regions, category, name, code):
        for region in get_area_keys(regions):
            cls.objects.get_or_create(model=model, region=region,
                category=category, name=name, code=code)

    def __str__(self):
        return '{}:{}:{}'.format(self.model, self.region, self.code)

    def __repr__(self):
        return self.__str__()

