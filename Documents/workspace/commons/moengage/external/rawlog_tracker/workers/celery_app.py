from celery import Celery
import moengage.external.rawlog_tracker.workers.celeryconfig as celeryconfig

app = Celery()
app.config_from_object(celeryconfig)

if __name__ == '__main__':
    app.start()
