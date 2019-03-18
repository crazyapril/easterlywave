"""Localized natural time processer

Refer: django.contrib.humanize.templatetags.humanize
"""
import calendar
import re
import datetime

from django import template
from django.template import defaultfilters
from django.utils.html import avoid_wrapping
from django.utils.timezone import is_aware, utc
from django.utils.translation import gettext as _
from django.utils.translation import (gettext_lazy, ngettext_lazy,
                                      npgettext_lazy)


class NaturalTimeFormatter:
    time_strings = {
        # Translators: delta will contain a string like '2月' or '1月, 2周'
        'past-day': gettext_lazy('%(delta)s前'),
        # Translators: please keep a non-breaking space (U+00A0) between count
        # and time unit.
        'past-hour': ngettext_lazy('一小时前', '%(count)s 小时前', 'count'),
        # Translators: please keep a non-breaking space (U+00A0) between count
        # and time unit.
        'past-minute': ngettext_lazy('一分钟前', '%(count)s 分钟前', 'count'),
        # Translators: please keep a non-breaking space (U+00A0) between count
        # and time unit.
        'past-second': ngettext_lazy('一秒前', '%(count)s 秒前', 'count'),
        'now': gettext_lazy('现在'),
        # Translators: please keep a non-breaking space (U+00A0) between count
        # and time unit.
        'future-second': ngettext_lazy('一秒后', '%(count)s 秒后', 'count'),
        # Translators: please keep a non-breaking space (U+00A0) between count
        # and time unit.
        'future-minute': ngettext_lazy('一分钟后', '%(count)s 分钟后', 'count'),
        # Translators: please keep a non-breaking space (U+00A0) between count
        # and time unit.
        'future-hour': ngettext_lazy('一小时后', '%(count)s 小时后', 'count'),
        # Translators: delta will contain a string like '2月' or '1月, 2周'
        'future-day': gettext_lazy('%(delta)s后'),
    }
    past_substrings = {
        # Translators: 'naturaltime-past' strings will be included in '%(delta)s ago'
        'year': npgettext_lazy('naturaltime-past', '%d年', '%d年'),
        'month': npgettext_lazy('naturaltime-past', '%d月', '%d月'),
        'week': npgettext_lazy('naturaltime-past', '%d周', '%d周'),
        'day': npgettext_lazy('naturaltime-past', '%d天', '%d天'),
        'hour': npgettext_lazy('naturaltime-past', '%d小时', '%d小时'),
        'minute': npgettext_lazy('naturaltime-past', '%d分钟', '%d分钟'),
    }
    future_substrings = {
        # Translators: 'naturaltime-future' strings will be included in '%(delta)s from now'
        'year': npgettext_lazy('naturaltime-future', '%d年', '%d年'),
        'month': npgettext_lazy('naturaltime-future', '%d月', '%d月'),
        'week': npgettext_lazy('naturaltime-future', '%d周', '%d周'),
        'day': npgettext_lazy('naturaltime-future', '%d日', '%d日'),
        'hour': npgettext_lazy('naturaltime-future', '%d小时', '%d小时'),
        'minute': npgettext_lazy('naturaltime-future', '%d分钟', '%d分钟'),
    }

    @classmethod
    def string_for(cls, value):
        if not isinstance(value, datetime.date):  # datetime is a subclass of date
            return value

        now = datetime.datetime.now(utc if is_aware(value) else None)
        if value < now:
            delta = now - value
            if delta.days != 0:
                return cls.time_strings['past-day'] % {
                    'delta': timesince(value, now, time_strings=cls.past_substrings),
                }
            elif delta.seconds == 0:
                return cls.time_strings['now']
            elif delta.seconds < 60:
                return cls.time_strings['past-second'] % {'count': delta.seconds}
            elif delta.seconds // 60 < 60:
                count = delta.seconds // 60
                return cls.time_strings['past-minute'] % {'count': count}
            else:
                count = delta.seconds // 60 // 60
                return cls.time_strings['past-hour'] % {'count': count}
        else:
            delta = value - now
            if delta.days != 0:
                return cls.time_strings['future-day'] % {
                    'delta': defaultfilters.timeuntil(value, now, time_strings=cls.future_substrings),
                }
            elif delta.seconds == 0:
                return cls.time_strings['now']
            elif delta.seconds < 60:
                return cls.time_strings['future-second'] % {'count': delta.seconds}
            elif delta.seconds // 60 < 60:
                count = delta.seconds // 60
                return cls.time_strings['future-minute'] % {'count': count}
            else:
                count = delta.seconds // 60 // 60
                return cls.time_strings['future-hour'] % {'count': count}


TIME_STRINGS = {
    'year': ngettext_lazy('%d year', '%d years'),
    'month': ngettext_lazy('%d month', '%d months'),
    'week': ngettext_lazy('%d week', '%d weeks'),
    'day': ngettext_lazy('%d day', '%d days'),
    'hour': ngettext_lazy('%d hour', '%d hours'),
    'minute': ngettext_lazy('%d minute', '%d minutes'),
}

TIMESINCE_CHUNKS = (
    (60 * 60 * 24 * 365, 'year'),
    (60 * 60 * 24 * 30, 'month'),
    (60 * 60 * 24 * 7, 'week'),
    (60 * 60 * 24, 'day'),
    (60 * 60, 'hour'),
    (60, 'minute'),
)

def timesince(d, now=None, reversed=False, time_strings=None):
    """
    Take two datetime objects and return the time between d and now as a nicely
    formatted string, e.g. "10 minutes". If d occurs after now, return
    "0 minutes".

    Units used are years, months, weeks, days, hours, and minutes.
    Seconds and microseconds are ignored.  Up to two adjacent units will be
    displayed.  For example, "2 weeks, 3 days" and "1 year, 3 months" are
    possible outputs, but "2 weeks, 3 hours" and "1 year, 5 days" are not.

    `time_strings` is an optional dict of strings to replace the default
    TIME_STRINGS dict.

    Adapted from
    https://web.archive.org/web/20060617175230/http://blog.natbat.co.uk/archive/2003/Jun/14/time_since
    """
    if time_strings is None:
        time_strings = TIME_STRINGS

    # Convert datetime.date to datetime.datetime for comparison.
    if not isinstance(d, datetime.datetime):
        d = datetime.datetime(d.year, d.month, d.day)
    if now and not isinstance(now, datetime.datetime):
        now = datetime.datetime(now.year, now.month, now.day)

    now = now or datetime.datetime.now(utc if is_aware(d) else None)

    if reversed:
        d, now = now, d
    delta = now - d

    # Deal with leapyears by subtracing the number of leapdays
    leapdays = calendar.leapdays(d.year, now.year)
    if leapdays != 0:
        if calendar.isleap(d.year):
            leapdays -= 1
        elif calendar.isleap(now.year):
            leapdays += 1
    delta -= datetime.timedelta(leapdays)

    # ignore microseconds
    since = delta.days * 24 * 60 * 60 + delta.seconds
    if since <= 0:
        # d is in the future compared to now, stop processing.
        return avoid_wrapping(_('0 minutes'))
    for i, (seconds, name) in enumerate(TIMESINCE_CHUNKS):
        count = since // seconds
        if count != 0:
            break
    result = avoid_wrapping(time_strings[name] % count)
    if i + 1 < len(TIMESINCE_CHUNKS):
        # Now get the second item
        seconds2, name2 = TIMESINCE_CHUNKS[i + 1]
        count2 = (since - (seconds * count)) // seconds2
        if count2 != 0:
            result += avoid_wrapping(time_strings[name2] % count2)
    return result


def naturaltime(s):
    return NaturalTimeFormatter.string_for(s)
