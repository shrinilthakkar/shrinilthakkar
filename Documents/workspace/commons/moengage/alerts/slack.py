from slacker import Slacker


class SlackMessageSender(object):
    API_TOKEN = 'xoxp-2542876939-15124078369-57168956835-95dfd68a81'
    ICON_URL = 'https://pixabay.com/static/uploads/photo/2014/03/25/15/17/cross-296395_960_720.png'
    USER_NAME = 'code-alert'
    DEFAULT_COLOR = '#F35A00'

    def __init__(self, channel_name, icon_url=None, user_name=None):
        self.channel_name = channel_name
        self.icon_url = icon_url or SlackMessageSender.ICON_URL
        self.user_name = user_name or SlackMessageSender.USER_NAME
        self.slack = Slacker(SlackMessageSender.API_TOKEN)

    def __messageDictEntryToField(self, key, value):
        from moengage.commons import CommonUtils
        val = CommonUtils.to_json(value)
        short = len(val) < 20
        return {
            'title': key,
            'value': val,
            'short': short
        }

    def __messageDictToAttachmentFields(self, **kwargs):
        return map(lambda x: self.__messageDictEntryToField(x, kwargs[x]), kwargs.keys())

    def __createAttachments(self, alert_level, alert_config, __tag_msg__="", **kwargs):
        message = __tag_msg__ + "\n" + alert_config.get_tag_names(alert_level)
        color = alert_config.get_color(alert_level)
        attachment = {
            'text': message,
            'color': color,
            'fields': self.__messageDictToAttachmentFields(**kwargs)
        }
        return [attachment]

    def sendTextMessage(self, message):
        self.slack.chat.post_message(self.channel_name, message, username=self.user_name, icon_url=self.icon_url,
                                     link_names=1)

    def sendDictMessage(self, message, alert_level, alert_config, **kwargs):
        icon_url = alert_config.get_icon_url(alert_level)
        attachments = self.__createAttachments(alert_level=alert_level, alert_config=alert_config, **kwargs)
        user_name = self.user_name + ('-' + str(alert_level))
        self.slack.chat.post_message(self.channel_name, message, username=user_name, icon_url=icon_url, link_names=1,
                                     attachments=attachments)
