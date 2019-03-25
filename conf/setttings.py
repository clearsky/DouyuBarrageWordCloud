import logging
import os
# mongodb设置
mongodb_setting = {
    'host': '127.0.0.1',
    'port': 27017,
    'db': 'test'
}

# 词云设置
word_cloud_setting = {
    'background_color': 'white',
    'font_path': 'C:\\WINDOWS\\FONTS\\MSJHL.TTC',
    'max_words': 2000,
    'max_font_size': 150,
    'random_state': 50,
    'width': 1920,
    'height': 1080,
    'scale': 1
}

# 斗鱼设置
douyu_setting = {
    'group': -9999,
    'host': 'openbarrage.douyutv.com',
    'port': 8601,
    'login_msg': 'type@=loginreq/roomid@={}/',
    'group_msg': 'type@=joingroup/rid@={}/gid@={}/',
    'heart_msg': 'type@=keeplive/tick@={}',
    'heart_time': 30,
    'cookie': 'dy_did=89af1771c0269c7ff0a46f3900031501; Hm_lvt_e0374aeb9ac41bee98043654e36ad504=1553325188,155332' \
                '5363,1553423798; acf_yb_did=89af1771c0269c7ff0a46f3900031501; _ga=GA1.2.1859474961.1553325190; Hm_' \
                 'lvt_e99aee90ec1b2106afe7ec3b199020a7=1553325196,1553423800; Hm_lpvt_e0374aeb9ac41bee98043654e36ad50' \
                 '4=1553423873; acf_yb_t=JkmQ586dzI54gC6YqtiC3WYGVPA0l5kj; _gid=GA1.2.706653098.1553423800; Hm_lpvt' \
                 '_e99aee90ec1b2106afe7ec3b199020a7=1553423800; smidV2=20190324183639fe911c0464cdc193038a1fee001f1217' \
                 '00971080aee423770; acf_yb_auth=63cb65580aced13b74d7d6ce9babcf2a22c9b28c; acf_yb_new_uid=LW7rOzjOo' \
                 'wGY; acf_yb_uid=31429419'
}

# 日志设置

logging_setting = {
    'version': 1,
    'formatters': {
        'stream_formatter': {
            'class': 'logging.Formatter',
            'format': '{asctime} - {levelname} - {message}',
            'style': '{'
        },
        'file_formatter': {
            'class': 'logging.Formatter',
            'format': '{asctime} - {levelname} - {message}',
            'style': '{'
        }
    },
    'handlers': {
        'stream_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'stream_formatter',
            'level': logging.INFO
        },
        'rotating_file_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(os.path.dirname(os.getcwd()), 'log/log'),
            'encoding': 'utf-8',
            'maxBytes': 4096,
            'backupCount': 10
        }
    },
    'loggers': {
        'douyu_logger': {
            'handlers': ['stream_handler'],
            'level': logging.INFO
        }
    }
}
logger_name = 'douyu_logger'

# 异步处理数据，取不到数据时的等待时间
async_sleep = 0.05

# 共同关注量最高的主播个数
high_follow_anchor_num = 10

# 获取关注列表关注主播的数量，关注主播少于设置值按实际值执行
get_follows_number = 200

# 房间号
room_id = 96291

# 要抓取的弹幕数量
barrage_num = 10

# 每个共同关注比较多的把主播房间抓取的弹幕数量
follow_barrage_num = 500

# 保存房间词云的路径
room_word_cloud_path = os.getcwd() + str(room_id) + "_room_word_cloud"

# 保存粉丝词云的路径
fans_word_cloud_path = os.getcwd() + str(room_id) + "_fans_word_cloud"
