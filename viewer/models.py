from django.core.cache import cache
from django.db import models

from tools.cache import redis_cached


# Create your models here.
class Station(models.Model):

    code = models.CharField(max_length=10, unique=True)
    name = models.CharField(max_length=32)
    location = models.CharField(max_length=32, null=True, blank=True)
    en_name = models.CharField(max_length=64, null=True, blank=True)
    lat = models.DecimalField(max_digits=7, decimal_places=5)
    lon = models.DecimalField(max_digits=8, decimal_places=5)
    altitude = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    hit = models.IntegerField(editable=False, default=0, blank=True)

    def __str__(self):
        return '{} {}'.format(self.code, self.name)

    class Meta:

        ordering = ['-hit']


class HitRecord(models.Model):

    name = models.CharField(max_length=32)
    date = models.DateField(auto_now_add=True)
    hit = models.IntegerField(editable=False, default=1, blank=True)

    def __str__(self):
        return '{} {} > {}'.format(self.date.strftime('%Y/%m/%d'), self.name, self.hit)

    class Meta:

        unique_together = ('name', 'date')
        ordering = ['-hit']


NOTICE_TYPES = ((1, 'good'), (2, 'neutral'), (3, 'bad'))

class Notice(models.Model):

    typ = models.IntegerField(choices=NOTICE_TYPES)
    ttl = models.IntegerField(default=360, blank=True) # in minutes
    start_time = models.DateTimeField(auto_now_add=True)
    content = models.CharField(max_length=256)

    def __str__(self):
        return '<Notice at {}>'.format(self.start_time)

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        cache.delete('notices') # clean cache

    def delete(self, *args, **kwargs):
        super().delete(*args, **kwargs)
        cache.delete('notices') # clean cache


class Switch(models.Model):

    name = models.CharField(max_length=32, unique=True)
    status = models.CharField(max_length=255)
    description = models.CharField(max_length=320, null=True, blank=True)
    last_changed = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        cache.delete('Switch.' + self.name) # clean cache

    @classmethod
    def get_status_by_name(cls, name):
        try:
            instance = cls.objects.get(name=name)
        except cls.DoesNotExist:
            return None
        else:
            return instance.status.upper()


class StationRecord(models.Model):

    code = models.CharField(max_length=10)
    item = models.CharField(max_length=16)
    month = models.IntegerField(default=0)
    rank = models.IntegerField()
    date = models.DateField()
    value = models.DecimalField(max_digits=7, decimal_places=2)
    valid = models.BooleanField(default=True)


class StationClimate(models.Model):

    code = models.CharField(max_length=10)
    month = models.IntegerField(default=0)
    tmax = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    tmin = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    precip = models.DecimalField(max_digits=6, decimal_places=2, null=True)


@redis_cached(namespace='Switch', timeout=3600)
def get_switch_status_by_name(name):
    return Switch.get_status_by_name(name)
