from django.db import models

from tools.mapstore import get_area_keys


class PlotModel(models.Model):

    category = models.CharField(max_length=32)
    name = models.CharField(max_length=64)
    code = models.CharField(max_length=16)
    region = models.CharField(max_length=24)
    region_url = models.CharField(max_length=24, blank=True)
    model = models.CharField(max_length=16)
    plevel = models.IntegerField(default=0)

    def save(self, **kwargs):
        self.region_url = self.region.replace(' ', '_').replace('&', '').lower()
        super().save(**kwargs)

    @classmethod
    def register(cls, model, regions, category, name, code, plevel):
        if plevel is None:
            plevel = [0 for i in range(regions)]
        elif isinstance(plevel, int):
            plevel = [plevel for i in range(regions)]
        for pl, re in zip(plevel, regions):
            for region in get_area_keys(re):
                cls.objects.get_or_create(model=model, region=region,
                    category=category, name=name, code=code, plevel=pl)

    def __str__(self):
        return '{}:{}:{}'.format(self.model, self.region, self.code)

    def __repr__(self):
        return self.__str__()

