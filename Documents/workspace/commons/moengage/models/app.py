from moengage.models.base import SchemaDocument, InvalidFieldValueException


class App(SchemaDocument):
    def __init__(self, **kwargs):
        self._name = None
        self._category = None
        self.no_of_downloads = None
        self.platforms = []
        self.createdDate = None
        self.db_name = None
        self.lower_db_name = None
        self.db_name2 = None
        self.app_key = None
        self.secret = None
        self.s2s_secret_access_key = None
        # to be used to with push
        self.gcm_sender_key = None
        self.wp_settings = {}

        # ios fields
        self.ios_prod_cert_file = None
        self.ios_prod_cert_pwd = None
        self.ios_dev_cert_file = None
        self.ios_dev_cert_pwd = None
        self.ios_dev_cert_filename = None
        # dev certificate expiry date
        self.ios_dev_cert_e_d = None

        # ipad fields
        self.ipad_cert_file = None
        self.ipad_cert_pwd = None
        self.ipad_cert_filename = None
        self.ipad_cert_e_d = None

        # Conversion Goal
        self.conv_goal = None
        # Conversion Goal attribute
        self.goal_attr = None
        self.package_name = None

        # Activation status
        self.activated = False
        # is the app live (are we getting any calls)
        self.is_live = False
        # is the app a test App
        self.is_test = True
        # if app is a testing account or production account
        self.is_prod = False

        # link to prod/test App
        self.account_link = None
        # limit for number of users to be created for a Test Account
        self.user_limit = 20000
        # 75% limit warning
        self.limit_notified = False
        # the platforms the app is live with
        self.live_platforms = []

        # If delay_optimization is enabled for db
        self.delay_optimization_set = False
        # If ST secondary Segmentation is enabled for db
        self.secondarySegmentationSt = False

        # In-App Campaigns V2 Settings
        self.default_inapp_delay = 15
        # In-App Presets Settings
        self.prst_settings = {}

        # flag that specifies if the river of that db_name is running
        self.is_river_enabled = False
        # TimeZone for User, Older Documents will be assumed to have IST as default timezone
        self.time_zone = None

        # Frequency cap, App level
        # frequencycap_max_pushes
        self.fc_m_p = None
        # frequencycap_max_pushes
        self.fc_t = None
        # frequencycap_override daywise
        self.fc_o = {}
        # frequencycap_timetolive
        self.fc_ttl = None
        # throttle speed in minutes, if 5 minutes, it means that we have to send push notifications in 5 minutes
        self.th_time = None
        # should refresh the FC daily instead of 24 hour basis
        self.fc_should_refresh_on_day = False

        # Windows App Settings
        self.client_id_windows = None
        self.client_secret_windows = None

        # Cloud front domain
        self.cloudfront_domain = None

        # email setting
        self.email_settings = {}

        # dnd setings
        self.is_dnd = False
        self.dnd_start_time = None
        self.dnd_end_time = None
        self.isQueued = False
        self.send_only_last_push = True
        self.send_staggered_push = False
        self.delay_staggered_push = None

        # ST frequency capping
        self.st_fc_m_p = 0
        self.st_fc_t = 1
        self.st_fc_should_refresh_on_day = False

        ### email campaign created or not
        self.em_cam_created = False

        # email frequency capping
        self.em_fc_m_p = 0
        self.em_fc_t = 1
        self.em_fc_should_refresh_on_day = False

        # dnd setings
        self.em_is_dnd = False
        self.em_dnd_start_time = None
        self.em_dnd_end_time = None
        self.em_isQueued = False
        self.em_send_only_last_push = True
        self.em_send_staggered_push = False
        self.em_delay_staggered_push = None

        # Flag set to turn on/off silent Pushes , added by @sameer19
        self.flag_run_silent_pushes = False
        # Flag set to enable calculation of Acquisition stats via ES, added by @sameer19
        self.flag_calculate_aq_es = False
        # Frequency to run silent pushes (days of week) , added by @sameer19
        self.silent_pushes_d_o_w = []
        # Time when uninstall settings were changed
        self.uninstall_settings_changed_time = None
        # Flag set to turn on/off iOS APNS feeback for uninstall tracking, added by @shreyakedia
        self.flag_track_ios_uninstalls = False
        # Flag set to turn on/off iOS silent pushes tracking for uninstall tracking, added by @shreyakedia
        self.flag_run_ios_silent_pushes = False

        # dict field for checking the integration
        self.integration = {}
        # blocked status for blocking the app
        self.blocked_status = False
        # daily dump settings
        self.daily_dump_settings = {}

        # 3rd party Attribution details to be saved here, @sameer19
        self.attribution_platform_settings = {}
        self.user_count = {}

        super(App, self).__init__(**kwargs)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if len(name) > 300:
            raise InvalidFieldValueException("App name should not be longer than 300 characters")
        self._name = name

    @property
    def category(self):
        return self._category

    @category.setter
    def category(self, cat):
        if len(cat) > 300:
            raise InvalidFieldValueException("App category should not be longer than 300 characters")
        self._category = cat
