import datetime
import os
import shutil

from celery import shared_task
from django.conf import settings


# Directories monitored by time in filename. Listed in (filedir, days_to_live)
# format. Cleaning work is executed daily in `daily_cleaner` task.
MONITOR_DIRS_BY_FILENAME = [
    (os.path.join(settings.MEDIA_ROOT, 'typhoon/ecens'), 4),
    (os.path.join(settings.MEDIA_ROOT, 'typhoon/sst'), 15),
    (os.path.join(settings.MEDIA_ROOT, 'model/ecmwf'), 3),
    (os.path.join(settings.PROTECTED_ROOT, 'model/ecmwf'), 3),
]

# Directories monitored by number of files under the directory. Listed in
# (filedir, clean_hour) format. If `clean_hour` is not None, the directory
# will only be checked at the specified hour of the day. Only the latest file
# will be preserved. Cleaning work is executed hourly in `hourly_cleaner` task.
MONITOR_DIRS_BY_ONLY_LATEST = [
    (os.path.join(settings.MEDIA_ROOT, 'sate'), 5),
    (os.path.join(settings.TMP_ROOT, 'sate'), None),
    (os.path.join(settings.TMP_ROOT, 'ecens'), None),
    (os.path.join(settings.TMP_ROOT, 'model'), None),
    (os.path.join(settings.MEDIA_ROOT, 'latest/satevid'), None),
]

# Directories monitored by last modified time (and often create time) of
# files under the directory. Listed in (filedir, days_to_live) format.
# Cleaning work is executed daily in `daily_cleaner` task.
MONITOR_DIRS_BY_MODIFY_TIME = [
    (os.path.join(settings.MEDIA_ROOT, 'latest/sate'), 30),
]


def monitor_by_modify_time():
    nowtime = datetime.datetime.now()
    for d, days in MONITOR_DIRS_BY_MODIFY_TIME:
        filenames = next(os.walk(d))[2]
        days_to_live = datetime.timedelta(days=days)
        for filename in filenames:
            full_filename = os.path.join(d, filename)
            modify_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(full_filename))
            if nowtime - modify_time > days_to_live:
                os.remove(full_filename)

def monitor_by_filename():
    nowtime = datetime.datetime.utcnow()
    for d, days in MONITOR_DIRS_BY_FILENAME:
        subdirs = [o for o in os.listdir(d) if os.path.isdir(os.path.join(d, o))]
        for sd in subdirs:
            if len(sd) == 8:
                sd_time = datetime.datetime.strptime(sd, '%Y%m%d')
            elif len(sd) == 10:
                sd_time = datetime.datetime.strptime(sd, '%Y%m%d%H')
            if nowtime - sd_time >= datetime.timedelta(days=days):
                shutil.rmtree(os.path.join(d, sd))

def monitor_by_only_latest():
    for d, t in MONITOR_DIRS_BY_ONLY_LATEST:
        if t is not None and datetime.datetime.utcnow().hour != t:
            continue
        subdirs = [o for o in os.listdir(d) if os.path.isdir(os.path.join(d, o))]
        subdirs.sort()
        for sd in subdirs[:-1]:
            shutil.rmtree(os.path.join(d, sd))


@shared_task(ignore_result=True)
def daily_cleaner():
    monitor_by_filename()
    monitor_by_modify_time()

@shared_task(ignore_result=True)
def hourly_cleaner():
    monitor_by_only_latest()
