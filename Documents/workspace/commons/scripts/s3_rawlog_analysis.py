import datetime

from moengage.commons.connections import ConnectionUtils
from moengage.daos.app_dao import AppDAO


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

    def getSizeForAllDbs(self, date_obj, app_filter={}):
        date_string = date_obj.isoformat()
        self.conn_to_s3_bucket()
        apps = AppDAO().find(app_filter)
        size_dict = {}
        app_keys = map(lambda app: app.app_key, apps)
        failed_dbs = []
        for app_key in app_keys:
            try:
                raw_log_prefix = 'secor_backup/rawlogs/' + date_string + '/appId=' + app_key + '/'
                print raw_log_prefix
                app_size_dict = {}
                size_dict[app_key] = app_size_dict
                for item in self.pushlogs_bucket.list(prefix=raw_log_prefix):
                    key_name = item.name
                    hour = key_name.rsplit('/', 2)[-2]
                    app_size_dict.setdefault(hour, 0)
                    app_size_dict[hour] += item.size
            except:
                failed_dbs.append(app_key)
        return size_dict, failed_dbs

if __name__ == "__main__":
    pass