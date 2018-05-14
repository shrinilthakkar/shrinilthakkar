from moengage.models.base import SchemalessDocument, SimpleDocument


class TimeBound(SimpleDocument):
    def __init__(self, **kwargs):
        self.start_date = None
        self.end_date = None
        self.start_time = None
        self.end_time = None
        super(TimeBound, self).__init__(**kwargs)


class CStats(SimpleDocument):
    def __init__(self, **kwargs):
        self.received = None
        self.clicked = None
        self.converted = None
        self.unique = None
        self.closed = None
        super(CStats, self).__init__(**kwargs)


class CampaignStats(SimpleDocument):
    def __init__(self, **kwargs):
        self.total = None
        self.segments = []
        super(CampaignStats, self).__init__(**kwargs)
        if self.total:
            self.total = CStats(**self.total)


class Campaign(SchemalessDocument):
    def __init__(self, **kwargs):
        # campaign name
        self.c_n = None
        # campaign type
        self.c_t = None
        # target audience segment
        self.c_t_s = None
        self.c_s_id = None
        self.c_s_f = None

        # platforms for which this campaign is created
        self.c_pl = []

        # push message
        self.c_pm = None
        # push message across platforms other than android
        self.c_pm_other = None
        # push screen to take the user to
        self.c_ps = None
        # dictionary for additional data for the screens to be sent with push notification
        self.c_ps_d = None
        # url to be shown
        self.c_p_url = None
        # push screen parameters for other platforms
        self.c_ps_other = None

        # delivery type - soon/now, later, repeat
        self.c_d_t = 'soon'

        # in-app self create template variable
        self.c_i_s = 0
        self.c_i_c = ''

        # other delivery parameters like date/time, day(for periodic campaigns), timezone
        # for later campaigns, date, time and timezone are present
        # for periodic campaigns, daily/weekly, days (sunday, tuesday, etc.),
        # time, timezone
        self.c_d_p = None

        # conversion goal event
        self.c_g = None

        # conversion goal attribute
        self.c_g_a = None
        self.c_g_a_q = None
        # attr_type (contains, between, greater, less, equal)
        self.c_g_a_t = None
        self.c_g_a_v = None  # attr_val ["string"] or [int1,int2]

        # conversion goal parameters if needed
        self.c_g_p = None


        # inAppMessaging Template params when required
        self.c_ia_p = None
        # In App Campaign for V2
        self.c_ia_p_v2 = None
        # flag for personalization of campaign
        self.is_personalizeV1 = False
        self.is_personalizeV2 = False

        # fallback message for inapp in case of personlization
        self.c_ia_p_v1_fallback = None
        self.c_ia_p_v2_fallback = None

        self.c_ia_metad = None
        self.c_ia_edit_dup = None

        # In App Campaign Stats V2
        # sample # {"0": {"type":"button","counter": {"a": 0,"i": 0,"w": 0}},
        # "1": {"type":"text","counter": {"a": 0,"i": 0,"w": 0}} }
        self.ia_c_stats = None

        # inAppMessaging expiry date
        self.c_ia_e_d = None
        # inAppMessaging/auto trigger  minimum interval betweeen messages
        self.c_ia_min_d = None
        # inAppMessaging maximum number of times to show the message
        self.c_ia_max_n = None
        # inAppMessaging full html , required for info page!
        self.c_ia_html = None
        # InAppCampaign v2 Dashboard JSON,required for info page as well as edit or duplicate campaign page
        self.c_ia_d_j = None

        # campaign version for any type of campaigns,for In App Campaign v2 we will use '1' as the version
        self.c_v = None

        # inApp last updated time
        self.c_ia_l_u = None

        # pause the campaign
        self.c_paused = False
        # stop the campaign
        self.c_stopped = False

        # image in push
        self.c_im_url = None
        # message type Image, Coupon
        self.c_m_t = None
        # coupon code fields
        self.cpn_t = None  # coupon type [generic,unique]
        self.cpn = None  # coupon code
        self.u_cpns = None  # unique coupons

        self.c_cr_t = None
        # campaign user count
        self.c_u_c = None
        # campaign user count android
        self.c_u_c_a = None
        # campaign user count windows
        self.c_u_c_w = None
        # campaign user count ios
        self.c_u_c_i = None
        # campaign user count web
        self.c_u_c_wb = None

        self.end_time = None
        self.status = 'Not Sent'
        self.stats = None
        self.segments = None

        # filters type ["AND","OR"]
        self.c_s_f_t = None
        # filters html
        self.c_s_f_h = None
        # last stats run time
        self.l_s_r_t = None

        # geo fencing related fields
        self.geo_fences = None
        self.time_between_alerts = 24       # days
        self.alerts_in_time = 1
        self.time_bounds = None

        # autotrigger fields
        # Campaign auto trigger frequency in minutes
        self.c_at_f = None
        # Campaign delay optimization from key delayOptimization and has min_t and max_t
        self.c_d_o = None
        # Dict field for trigger params of autotrigger feature.
        self.c_at_trigger = None
        # Campaign actions, to send autotrigger
        self.c_at_act = None
        # trigger frequency multiplier
        self.c_at_f_m = None
        # is web hook enabled
        self.is_wh_en = False
        # webhook url, set only when webhook is enabled
        self.wh_url = None
        self.wh_fields = None
        self.wh_stats = None
        # TTL, in seconds
        self.c_ttl = None

        # Soucre from which campaign is created
        self.c_src = None

        # parent campaign id added for the purpose of recurring campaigns
        self.c_p_id = None
        self.c_chld_ids = None

        # email campaign fields - added by Sandeep
        # data includes - email body # email template name
        # email sender name # email from id # email reply id
        # email subject
        self.em_data = None

        # personalization fallback_fields - added by Hitesh
        # personalization fallback - personalization fallback enabled boolean,
        # title, message, screenname, kv pairs, deeplinking url
        self.c_per_f_f = None

        # is personalized flag
        self.c_f_is_per = None  # is_a_per,is_i_per,is_w_per,is_b_per
        self.c_con_avail = 0

        # campaign stats
        self.c_stats = None

        # segmentation field in Campaign Creation for population
        # added by swapnil for pre populating segmentation in edit/duplicate campaigns
        self.c_seg_new = None

        # flag is Archived. Added by @sameer19 for archiving / unarchiving campaigns
        self.c_f_is_a = False
        # flag is test. Added by @sameer19 for test/regular campaigns
        self.c_f_is_t = False
        # Campaign Tags (Predefined and cannot change). Added by @sameer19
        self.c_tg = None
        # Campaign User defined Tags. Added by @sameer19
        self.c_u_tg = None
        self.c_start_time = None

        # Campaign Flag for checking whether frequency cap in enabled or not . Added by @amithood
        self.c_f_is_fc = False
        # Campaign Flag for checking whether in case of disabled frequency cap,
        # push should be counted in frequency Cap or not
        self.c_f_is_fc_count = True

        # update bloom filter related information, only for inapp campaigns
        self.bloom_update_info = None

        # upload flag for filepicker, set when user uploads a file , unset when use url itself
        self.upload_image_flag = False

        # for smart triggers only
        self.bypass_dnd = False

        # personalize email campaigns field..
        self.subject_name_personalization_dict = None
        # campaign user count with email field:
        self.u_c_e = None
        # campaign user count containing emails and not HB/S/U
        self.c_e_u_c = None
        # actions in segmentation for the campaign
        self.actions = None

        self.json_data = None
        # object ID of the CampaignsData collections.

        self.c_g_m = None

        self._c_g_t_i = 86400   # max_value=129600

        self._c_g_t_c = 86400   # max_value=129600

        # variation percentage
        self.var_p = None

        # list of variate keys
        self.c_v_keys = []

        # aclrole
        self.role = "Admin"
        super(Campaign, self).__init__(**kwargs)

        if self.stats:
            self.stats = CampaignStats(**self.stats)

        if self.time_bounds:
            bounds = []
            for bound in self.time_bounds:
                time_bound = TimeBound(**bound)
                bounds.append(time_bound)
            self.time_bounds = bounds