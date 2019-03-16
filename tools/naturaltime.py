"""Localized natural time processer

Refer: django.contrib.humanize.templatetags.humanize
"""
import re
from datetime import date, datetime
from decimal import Decimal

from django import template
from django.conf import settings
from django.template import defaultfilters
from django.utils.formats import number_format
from django.utils.safestring import mark_safe
from django.utils.timezone import is_aware, utc
from django.utils.translation import gettext as _
from django.utils.translation import (gettext_lazy, ngettext, ngettext_lazy,
                                      npgettext_lazy, pgettext)


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
        'now': gettext_lazy('now'),
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
        'future-day': gettext_lazy('%(delta)s from now'),
    }
    past_substrings = {
        # Translators: 'naturaltime-past' strings will be included in '%(delta)s ago'
        'year': npgettext_lazy('naturaltime-past', '%d年', '%d年'),
        'month': npgettext_lazy('naturaltime-past', '%d月', '%d月'),
        'week': npgettext_lazy('naturaltime-past', '%d周', '%d周'),
        'day': npgettext_lazy('naturaltime-past', '%d日', '%d日'),
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
        if not isinstance(value, date):  # datetime is a subclass of date
            return value

        now = datetime.now(utc if is_aware(value) else None)
        if value < now:
            delta = now - value
            if delta.days != 0:
                return cls.time_strings['past-day'] % {
                    'delta': defaultfilters.timesince(value, now, time_strings=cls.past_substrings),
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


def naturaltime(s):
    return NaturalTimeFormatter.string_for(s)
