from moengage.models.base import SchemalessDocument


class User(SchemalessDocument):
    def __init__(self, **kwargs):
        # user name
        self.u_n = None
        # user first name
        self.u_fn = None
        # user last name
        self.u_ln = None
        # user email
        self.u_em = None
        # user gender
        self.u_gd = None
        # user mobile number
        self.u_mb = None
        # user birthday
        self.u_bd = None
        # account id
        self.uid = None
        self.geo = None
        # user created time
        self.cr_t = None
        # user session count
        self.u_s_c = 1
        # user last active
        self.u_l_a = None
        # user last notification received
        self.moe_u_l_n_r = None
        self.t_trans = 0
        self.t_rev = 0
        self.status = 0
        self.src = None
        self.installed = True
        self.devices = []
        # total devices mapped to this user
        self.moe_d_c = None
        self.os = []
        self.u_rev = []
        self.u_trans = []
        self.u_sess = []
        # non general push campaigns
        self.u_campaigns = []
        # general push campaigns
        self.g_campaigns = []
        # API Type
        self.api_t = None
        # whether the user is new or not, set to True on insert, set to False on first save report call
        self.is_new_user = None
        self.old_user_id = None
        # stores whether the geo is saved as lat,lng(False) or lng,lat(True)
        self.is_geo_reversed = False

        # email settings for users - hardbounced, softbounced, no of attempts, spam
        self.moe_hard_bounce = False
        self.moe_unsubscribe = False
        self.moe_spam = False
        self.moe_invalid_email = None
        self.moe_em_notif = {}

        # acl flag for detecting a test user
        self.moe_test_device = False

        super(User, self).__init__(**kwargs)
