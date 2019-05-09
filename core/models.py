from django.contrib.auth.models import User
from django.db import models
from django.utils.timezone import now

PLEVELS = [0, 1]

class Privilege(models.Model):

    uid = models.PositiveIntegerField(default=0)
    username = models.CharField(max_length=64)
    level = models.IntegerField(default=0)
    expire_time = models.DateTimeField(null=True, blank=True)
    active_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return '{} > {}'.format(self.username, self.level)

    @classmethod
    def check_level(cls, uid):
        try:
            privilege = cls.objects.get(uid=uid)
        except cls.DoesNotExist:
            try:
                user = User.objects.get(pk=uid)
            except User.DoesNotExist:
                return 0
            else:
                cls.objects.create(uid=uid, username=user.username, expire_time=None)
                return 0
        if privilege.expire_time > now():
            privilege.level = 0
            privilege.expire_time = None
            privilege.save()
        return privilege.level
