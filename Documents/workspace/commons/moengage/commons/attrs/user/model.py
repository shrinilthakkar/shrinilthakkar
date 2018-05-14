from moengage.commons.attrs.attribute_model import MOEAttribute
from moengage.commons.attrs.config.provider import UserAttrInfoProvider
from moengage.commons.attrs.data_type import DataType


class MOEUserAttribute(MOEAttribute):
    DEFAULT_CATEGORY = 'Tracked User Attribute'
    DEFAULT_ATTRIBUTES = [
        MOEAttribute(name='uid', data_types=[DataType.STRING]),
        MOEAttribute(name='u_s_c', data_types=[DataType.DOUBLE]),
        MOEAttribute(name='u_l_a', data_types=[DataType.DATETIME]),
        MOEAttribute(name='cr_t', data_types=[DataType.DATETIME]),
        MOEAttribute(name='moe_sub_w', data_types=[DataType.BOOL], platform='web'),
        MOEAttribute(name='moe_w_ds', data_types=[DataType.STRING], platform='web'),
        MOEAttribute(name='moe_mweb', data_types=[DataType.BOOL], platform='web'),
        MOEAttribute(name='uninstall_time', data_types=[DataType.DATETIME], platform='iOS'),
        MOEAttribute(name='uninstall_time', data_types=[DataType.DATETIME], platform='ANDROID'),
        MOEAttribute(name='uninstall_time', data_types=[DataType.DATETIME], platform='Windows'),
        MOEAttribute(name='publisher_name', data_types=[DataType.STRING]),
        MOEAttribute(name='campaign_name', data_types=[DataType.STRING]),
        MOEAttribute(name='t_rev', data_types=[DataType.DOUBLE]),
        MOEAttribute(name='t_trans', data_types=[DataType.DOUBLE]),
        MOEAttribute(name='moe_ip_city', data_types=[DataType.STRING]),
        MOEAttribute(name='moe_ip_pin', data_types=[DataType.DOUBLE]),
        MOEAttribute(name='moe_ip_subdivision', data_types=[DataType.STRING]),
        MOEAttribute(name='moe_ip_country', data_types=[DataType.STRING]),
        MOEAttribute(name='moe_dtzo', data_types=[DataType.DOUBLE]),
        MOEAttribute(name='MOE_GAID', data_types=[DataType.STRING], platform='ANDROID'),
        MOEAttribute(name='moe_i_ov', data_types=[DataType.STRING], platform='iOS'),
        MOEAttribute(name='installed', data_types=[DataType.BOOL]),
        MOEAttribute(name='ADVERTISING_IDENTIFIER', data_types=[DataType.STRING], platform='iOS'),
        MOEAttribute(name='moe_cr_from', data_types=[DataType.STRING]),
        MOEAttribute(name='moe_spam', data_types=[DataType.BOOL]),
        MOEAttribute(name='moe_hard_bounce', data_types=[DataType.BOOL]),
        MOEAttribute(name='moe_unsubscribe', data_types=[DataType.BOOL]),
        MOEAttribute(name='u_em', data_types=[DataType.STRING])
    ]
    INFO_PROVIDER = UserAttrInfoProvider()
