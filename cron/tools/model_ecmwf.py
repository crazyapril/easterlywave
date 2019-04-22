import django
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(root)
sys.path.append(root)
os.environ['DJANGO_SETTINGS_MODULE'] = 'windygram.settings'

django.setup()


from model.kicker import plot_ecmwf

plot_ecmwf()

