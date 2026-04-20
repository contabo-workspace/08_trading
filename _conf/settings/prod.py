import os

from .base import *

DEBUG = False
ALLOWED_HOSTS = ['185.135.137.100', 'trading.hs-dev.cz', 'localhost', '127.0.0.1']

SECRET_KEY = os.getenv('SECRET_KEY', SECRET_KEY)

STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_DIRS = [BASE_DIR / 'static']

MIDDLEWARE.insert(1, 'whitenoise.middleware.WhiteNoiseMiddleware')


