import os 
from celery import Celery


# Set default Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tick_system.settings')

app = Celery('tick_system')

# Load settings from Django, using CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps. Auto discovering tasks
app.autodiscover_tasks()