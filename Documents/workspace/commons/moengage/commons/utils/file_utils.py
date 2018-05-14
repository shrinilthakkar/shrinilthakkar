import requests
from requests.auth import HTTPBasicAuth


class FileUtils(object):
    def __init__(self, file_path):
        self.file_path = file_path

    def downloadFromUrl(self, web_url, auth_username=None, auth_password=None):
        params = {}
        if auth_username and auth_password:
            params['auth'] = HTTPBasicAuth(username=auth_username, password=auth_password)
        r = requests.get(web_url, **params)
        if r.status_code == requests.codes.ok:
            with open(self.file_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=1024):
                    fd.write(chunk)
        return r.status_code
