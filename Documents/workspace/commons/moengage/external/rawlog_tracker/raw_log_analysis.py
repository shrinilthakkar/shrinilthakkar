import datetime
from datetime import timedelta
from moengage.commons.connections import ConnectionUtils
from moengage.daos.app_dao import AppDAO
from moengage.external.rawlog_tracker.model import RawLogTracker
from moengage.external.rawlog_tracker.dao import RawLogTrackerDao
from moengage.commons.loggers import Treysor
from moengage.commons.decorators import WatchdogMetricRecorder


class GetS3Data(object):
    def __init__(self):
        self.conn_s3 = ConnectionUtils.getS3Connection()
        self.s3_data = dict()
        self.app_ids = dict()
        self.pushlogs_bucket = None
        self.elastic_search_bucket = None
        self.conn_to_s3_bucket()

    @staticmethod
    def bucket_size_in_gb(bucket_object):
        total_size = 0
        for obj in bucket_object.list():
            total_size += obj.size
        return total_size * 1.0 / (1024 ** 3)

    @staticmethod
    def is_valid_date(bucket_name):
        date_string = bucket_name.split('/')[2]
        day = int(date_string.split("-")[2])
        month = int(date_string.split("-")[1])
        year = int(date_string.split("-")[0])
        date_today = datetime.datetime.today()
        return True if date_today.month == month and date_today.year == year else False

    def _handle_raw_logs(self, raw_log_object):
        if not self.is_valid_date(raw_log_object.name):
            return 0
        return raw_log_object.size

    def _handle_raw_push_logs(self, raw_push_log_object):
        return raw_push_log_object.size

    def conn_to_s3_bucket(self):
        self.pushlogs_bucket = self.conn_s3.get_bucket('moe-pushlogs')

    def getSizeForAllDbs(self, app_filter=None):
        if app_filter is None:
            app_filter = {}
        rawlog_dao = RawLogTrackerDao()
        date_today = datetime.datetime.utcnow()
        date_prev = datetime.datetime.utcnow() - timedelta(days=30)
        day_today, month_today, year_today = date_today.day, date_today.month, date_today.year
        hour_current = date_today.hour
        self.conn_to_s3_bucket()
        apps = AppDAO().find(app_filter)
        size_dict = {}
        failed_dbs = []
        app_keys = map(lambda app: app.app_key, apps)
        for app_key in app_keys:
            try:
                app_size_dict = {}
                raw_log = rawlog_dao.findOne({'app_key': app_key}) or RawLogTracker()
                start_date = raw_log.last_updated_date or date_prev
                end_date = date_today
                date_updated, hour_updated = start_date, 0
                single_date = start_date
                while single_date <= end_date:
                    strdate = single_date.strftime('%Y-%m-%d')
                    end_hour = hour_current - 2 if single_date == day_today else 24
                    start_hour = raw_log.last_updated_hour
                    for hour in range(start_hour, end_hour):
                        strhour = '0' + str(hour) if hour <= 9 else str(hour)
                        raw_log_prefix = 'secor_backup/rawlogs/' + strdate + '/appId=' + app_key + '/h=' + strhour + '/'
                        size_dict[app_key] = app_size_dict
                        app_size_dict.setdefault(hour, 0)
                        for item in self.pushlogs_bucket.list(prefix=raw_log_prefix):
                            app_size_dict[hour] += (item.size/1024)
                            hour_updated, date_updated = hour, single_date
                        WatchdogMetricRecorder('log.rawlog.size_dict', tags={'app_key': app_key, 'date': date_updated,
                                                            'hour': hour, 'size': app_size_dict[hour]}).record()
                    single_date += timedelta(days=1)
                raw_log.last_updated_date, raw_log.last_updated_hour = date_updated, hour_updated
                raw_log.app_key = app_key
                rawlog_dao.save(raw_log)
            except Exception as e:
                Treysor().to_json({"app_key": app_key, "error_reason": repr(e)})
                failed_dbs.append(app_key)
                break


if __name__ == "__main__":
    pass
