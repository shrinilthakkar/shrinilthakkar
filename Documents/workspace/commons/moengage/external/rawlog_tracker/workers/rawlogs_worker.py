from moengage.external.rawlog_tracker.raw_log_analysis import GetS3Data
from moengage.external.rawlog_tracker.workers.celery_app import app


@app.task
def calculate_size_rawlog():
    s3_data_obj = GetS3Data()
    s3_data_obj.getSizeForAllDbs()
