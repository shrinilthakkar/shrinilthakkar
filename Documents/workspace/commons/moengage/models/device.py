from moengage.models.base import SchemalessDocument


class Device(SchemalessDocument):
    def __init__(self, **kwargs):
        self.OS_VERSION_INCR = None
        self.product = None
        self.channel_uri = None
        self.push_token = None
        self.os_key = None
        self.OS_VERSION = None
        # mobile model: NOKIA Lumia 520? Micromax A116?
        self.MODEL = None
        # Device short code?
        self.device = None
        # Os api level/api version : Android Api version 16, 20 etc
        self.OS_API_LEVEL = None
        # account ID which device is connected to
        self.account = None
        # Merging the 3 push ids to 1 push id to make it easier to index if required and to maintain consistency
        self.push_id = None
        # the app version given by vendor: 3.0.0? 40?
        self.app_version = None
        # Unique id sent from device. Starting from apiv2.0 this is compulsory
        self.unique_id = None
        # id of user object associated with this device UserObj.id
        self.user_id = None
        # Type of SDK specially for windows to differentiate between WinRT and Silverlight framework
        self.sdk_type = None
        self.created_time = None
        self.update_time = None
        self.last_push_time = None
        self.user = None
        self.uid = None
        # API Type
        self.api_t = None
        self.status = 1
        super(Device, self).__init__(**kwargs)
