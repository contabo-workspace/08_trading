import os

from .base import *

DEBUG = False
ALLOWED_HOSTS = ['185.135.137.100', 'trading.hs-dev.cz', 'localhost', '127.0.0.1']

SECRET_KEY = os.getenv('SECRET_KEY', SECRET_KEY)

# HTTPS / SSL
SECURE_SSL_REDIRECT = True
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_HSTS_SECONDS = 31536000
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True

STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_DIRS = [BASE_DIR / 'static']

MIDDLEWARE.insert(1, 'whitenoise.middleware.WhiteNoiseMiddleware')


