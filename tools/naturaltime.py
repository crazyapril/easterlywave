import datetime

from django.utils.timezone import is_aware, utc


class NaturalTimeFormatter:

    MINUTE = 60
    HOUR = 60 * 60
    DAY = 60 * 60 * 24
    _30_DAY = 60 * 60 * 24 * 30
    YEAR = 60 * 60 * 24 * 365

    @classmethod
    def string_for(cls, value):
        if not isinstance(value, datetime.date):  # datetime is a subclass of date
            return value
        now = datetime.datetime.now(utc if is_aware(value) else None)
        if value < now:
            delta = now - value
            delta_seconds = int(delta / datetime.timedelta(seconds=1))
            if delta_seconds < cls.MINUTE:
                return '刚刚'
            if delta_seconds < cls.HOUR:
                minutes = delta_seconds // cls.MINUTE
                if minutes == 1:
                    return '一分钟前'
                return '{}分钟前'.format(minutes)
            if delta_seconds < cls.DAY:
                hours = delta_seconds // cls.HOUR
                if hours == 1:
                    return '一小时前'
                return '{}小时前'.format(hours)
            if delta_seconds < cls._30_DAY:
                days = delta_seconds // cls.DAY
                if days == 1:
                    return '一天前'
                return '{}天前'.format(days)
            return value.strftime('%Y{}%m{}%d{}').format('年', '月', '日')


def naturaltime(s):
    return NaturalTimeFormatter.string_for(s)
