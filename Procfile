web: gunicorn app:server --workers 4 
worker: celery -A app:celery_app worker --loglevel=INFO --concurrency=2