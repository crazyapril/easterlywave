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

app.conf.update(
    CELERYBEAT_SCHEDULE = {
        'sate-normal-plotter': {
            'task': 'sate.tasks.plotter',
            'schedule': crontab(minute=[
                2, 4, 8, 10,
                12, 14, 18, 20,
                22, 24, 28, 30,
                32, 34, 38, 40,
                42, 44, 48, 50,
                52, 54, 58, 0
            ])
        },
        'sate-fulldisk-plotter': {
            'task': 'sate.tasks.fulldisk_plotter',
            'schedule': crontab(minute=[5, 15, 25, 35, 45, 55])
        },
        'ec-ensemble-plotter': {
            'task': 'sate.ecens.plot_ec_ensemble',
            'schedule': crontab(hour='7,19', minute=55)
        },
        'sate-data-cleaner': {
            'task': 'sate.tasks.cleaner',
            'schedule': crontab(minute=15)
        },
        'daily-data-cleaner': {
            'task': 'sate.tasks.date_cleaner',
            'schedule': crontab(hour=0, minute=10)
        }
    }
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
