import socket
import requests
import re
import pymongo
import time
import json
import logging.config
from threading import Thread
from datetime import datetime
from conf.setttings import mongodb_setting, douyu_setting, logging_setting, logger_name


class RoomError(Exception):
    """
    自定义错误类型，主播未开播时抛出
    """
    def __init__(self, error_info):
        super().__init__(self)
        self.error_info = error_info

    def __str__(self):
        return self.error_info



class GetDouyuBarrage:
    """
    连接斗鱼弹幕服务器，获取弹幕、发弹幕的用户用户名和用户id，并存储到mongodb
    """

    def __init__(self, room_id, *args, barrage_num):
        logging.config.dictConfig(logging_setting)
        self.logger = logging.getLogger(logger_name)  # 获取logger
        self.room_id = room_id  # 设置房间号
        self.nick_name = None  # 主播名
        self.check_room_is_open()  # 检查房间是否开播并设置nick_name
        self.group = douyu_setting['group']  # 设置弹幕分组，-9999为海量弹幕分组
        self.barrage_num = barrage_num  # 设置要抓取的弹幕的条数
        self.msg_list = []  # 消息列表，存放从弹幕服务器获取的消息
        self.result_list = []  # 存放解析后的数据，用于存储和输出
        self.finish = False   # 用以结束发送心跳的线程

        self.logger.info('正在初始化mongodb,房间:{},主播:{}'.format(self.room_id, self.nick_name))  # 初始化mongo数据库
        mongo_host = mongodb_setting['host']
        mongo_port = mongodb_setting['port']
        self.mongo_db_name = mongodb_setting['db']
        self.mongo_db_collection = 'douyu{}{}'.format(self.room_id, self.nick_name)
        self.mongo_conn = pymongo.MongoClient(host=mongo_host, port=mongo_port)
        self.mongo = self.mongo_conn[self.mongo_db_name][self.mongo_db_collection]
        self._id = self.mongo.find({}).count()  # 写入数据的id，写入一条数据，增加一个
        self.doc_num = self._id
        self.logger.info('mongodb初始化成功,房间:{},主播:{}'.format(self.room_id, self.nick_name))

        self.logger.info('正在连接弹幕服务器,房间:{},主播:{}'.format(self.room_id, self.nick_name))  # 连接弹幕服务器
        douyu_host = douyu_setting['host']
        douyu_port = douyu_setting['port']
        self.client = socket.socket()
        self.client.connect((douyu_host, douyu_port))
        self.logger.info('弹幕服务器连接成功,房间:{},主播:{}'.format(self.room_id, self.nick_name))

    def get_room_info(self):
        """
        获取斗鱼房间信息
        :return: json格式的房间信息
        """

        url = 'http://www.douyu.com/betard/{}'.format(self.room_id)
        resopnse = requests.get(url=url)
        data = resopnse.text.strip()
        if data:
            try:
                data = json.loads(data)
            except Exception:
                return 
            resopnse.close()
            return data

    def check_room_is_open(self):
        """
        判断当前房间是否开播,并设置主播名，没开播抛出RoomError异常
        :return:
        """
        data = self.get_room_info()  # 获取斗鱼房间信息
        if data:
            is_open = data['room']['show_status']
            self.nick_name = data['room']['nickname']
            self.logger.info('正在检查房间:{},主播:{}是否开播'.format(self.room_id, self.nick_name))
            if is_open == 2:
                self.finish = True
                raise RoomError('主播{}未开播, 房间号:{}'.format(self.nick_name, self.room_id))  # 没开播抛出异常
            else:
                self.logger.info('房间:{},主播:{}已开播'.format(self.room_id, self.nick_name))

    def send(self, msg):
        """
        按照协议，发送消息
        :param msg:消息
        :return:
        """
        self.logger.info('开始发送数据，房间:{},主播:{}'.format(self.room_id, self.nick_name))
        msg = msg + '\0'  # 消息需要以\0结尾
        msg = msg.encode('utf-8')
        type_code = 689  # 消息类型
        length = len(msg) + 8
        header = int.to_bytes(length, 4, 'little')+int.to_bytes(length, 4, 'little') + \
            int.to_bytes(type_code, 4, 'little')  # 构造消息头
        while True:
            try:
                self.client.send(header+msg)
                break
            except Exception:
                if self.finish:
                    break
                self.logger.info('发送数据失败，房间:{},主播:{}'.format(self.room_id, self.nick_name))
                self.reconnect()

    def reconnect(self):
        """
        掉线重连
        :return:
        """
        self.logger.info('正在连接重新连接弹幕服务器,房间:{},主播:{}'.format(self.room_id, self.nick_name))  # 连接弹幕服务器
        self.client.close()
        douyu_host = douyu_setting['host']
        douyu_port = douyu_setting['port']
        self.client = socket.socket()
        self.client.connect((douyu_host, douyu_port))
        self.login()
        self.logger.info('重新连接弹幕服务器连接成功,房间:{},主播:{}'.format(self.room_id, self.nick_name))

    def login(self):
        """
        发送登录请求和加组请求
        :return:
        """
        self.logger.info('正在登录分组 房间:{},主播:{}'.format(self.room_id, self.nick_name))
        login_msg = douyu_setting['login_msg'].format(self.room_id)  # 发送登录消息
        self.send(login_msg)
        group_msg = douyu_setting['group_msg'].format(self.room_id, self.group)  # 发送加组消息
        self.send(group_msg)
        self.logger.info('登录分组成功成功 房间:{},主播:{}'.format(self.room_id, self.nick_name))

    def get_msg(self):
        """
        从弹幕服务器获取消息，并将获取到的消息放入msg_list当中
        :return:
        """
        try:
            data_from_douyu = self.client.recv(1024)
        except Exception:
            self.logger.info('获取消息失败 房间:{},主播:{}'.format(self.room_id, self.nick_name))
            raise Exception
        self.msg_list.append(data_from_douyu)

    def parse_data(self):
        """
        从弹幕服务器获取的消息中，解析出弹幕、发送弹幕的用户名和用户id，并加上数据的id、time
        转换成一个字典列表，并添加到result_list当中
        :return:
        """
        try:
            data = self.msg_list.pop()
        except Exception:
            self.logger.info('获取弹幕失败 房间:{},主播:{}'.format(self.room_id, self.nick_name))
            raise Exception

        data = data.decode('utf-8', 'ignore')  # 消息转码
        if re.search(r'type@=chatmsg', data):  # 是否存在chatmsg类型消息，chatmsg是文字弹幕消息
            data = re.findall(r'type@=chatmsg.+?txt@=.+?/', data)
            data = list(map(lambda x: re.search('uid@=.+/', x).group(), data))
            data = list(map(lambda x: x[:-1].split('/', 2), data))  # split第二个参数是2，防止弹幕中有/出错
            data = list(map(lambda x: list(map(lambda y: y.split('@=', 1), x)), data))  # split第二个参数是1，防止弹幕中有@=出错
            data = list(map(lambda x: dict(x), data))
            for data_line in data:
                data_line['time'] = datetime.now()  # 添加时间
                data_line['_id'] = self._id  # 添加id
                self._id += 1  # id自增
            [self.logger.info('{},主播:{},房间号:{},{}'.format(str(data_line), self.nick_name, self.room_id, self._id - self.doc_num)) for data_line in data]
            self.result_list.append(data)

    def save_data(self):
        """
        从result_list获取数据并存储
        :return:
        """
        result_list = []
        try:
            result_list = self.result_list.pop()
        except Exception:
            pass  # parse_data中可能没有文字弹幕类型消息，所以此错误不做处理
        if result_list:
            self.mongo.insert(result_list)

    def keep_live(self):
        """
        定时发送心跳，保持弹幕服务器的连接
        并检查当前房间是否开播
        :return:
        """
        while not self.finish:
            keep_msg = douyu_setting['heart_msg'].format(str(int(time.time())))  # 心跳消息
            self.logger.info('发送心跳,主播:{},房间号:{}'.format(self.nick_name, self.room_id))
            self.send(keep_msg)
            self.logger.info('心跳发送完成,主播:{},房间号:{}'.format(self.nick_name, self.room_id))
            time.sleep(douyu_setting['heart_time'])
            self.check_room_is_open()  # 检测当前房间是否开播

    def run(self):
        """
        启动函数
        :return:
        """
        self.login()  # 登录房间和分组
        t = Thread(target=self.keep_live)
        t.daemon = True
        t.start()
        while not self.finish:
            self.get_msg()
            self.parse_data()
            self.save_data()
            time.sleep(0.05)
            if self._id - self.doc_num >= self.barrage_num:
                break
        self.finish = True
        self.client.close()  # 关闭socket连接
        self.mongo_conn.close()  # 关闭mongodb连接


if __name__ == '__main__':
    d = GetDouyuBarrage(96291, barrage_num=500)
    d.run()
