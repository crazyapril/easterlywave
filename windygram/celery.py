from __future__ import absolute_import

import os

from celery import Celery, platforms
from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'windygram.settings')

app = Celery('windygram')

# Using a string here means the worker don't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

__mode__ = 'normal'
schedules = {
    'sate-normal-plotter': {
        'task': 'sate.tasks.plotter',
        'schedule': crontab(minute=[
            3, 5, 9, 11,
            13, 15, 19, 21,
            23, 25, 29, 31,
            33, 35, 39, 41,
            43, 45, 49, 51,
            53, 55, 59, 1
        ])
    },
    'sate-fulldisk-plotter': {
        'task': 'sate.tasks.fulldisk_plotter',
        'schedule': crontab(minute=[6, 16, 26, 36, 46, 56])
    },
    'ec-ensemble-plotter': {
        'task': 'sate.ecens.plot_ec_ensemble',
        'schedule': crontab(hour='1,7,13,19', minute=47)
    },
    'rtofs-sst-plotter': {
        'task': 'sate.rtofs.plot_rtofs_sst',
        'schedule': crontab(hour=3, minute=36)
    },
    'realtime-map-plotter': {
        'task': 'viewer.rtmap.plot_realtime_map',
        'schedule': crontab(minute=18)
    },
    'sate-data-cleaner': {
        'task': 'sate.cleaner.hourly_cleaner',
        'schedule': crontab(minute=15)
    },
    'daily-data-cleaner': {
        'task': 'sate.cleaner.daily_cleaner',
        'schedule': crontab(hour=0, minute=10)
    }
}
master_schedules = [
    'ec-ensemble-plotter',
    'rtofs-sst-plotter',
    'realtime-map-plotter',
    'daily-data-cleaner'
]
slave_schedules = [
    'sate-normal-plotter',
    'sate-fulldisk-plotter',
    'sate-data-cleaner',
    'daily-data-cleaner'
]
if __mode__ == 'master':
    schedules = {k:v for k, v in schedules.items() if k in master_schedules}
elif __mode__ == 'slave':
    schedules = {k:v for k, v in schedules.items() if k in slave_schedules}

app.conf.update(
    CELERYBEAT_SCHEDULE = schedules
)
app.conf.worker_concurrency = 1
app.conf.worker_max_tasks_per_child = 24

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# 允许root 用户运行celery
#platforms.C_FORCE_ROOT = True


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
