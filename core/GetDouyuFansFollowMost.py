import pymongo
import asyncio
import aiohttp
from lxml import etree
import time
import logging.config
from conf.setttings import mongodb_setting, high_follow_anchor_num, logging_setting, logger_name, room_id

class GetDouyuFansFollowMost:
    def __init__(self, *args, db, collection):
        logging.config.dictConfig(logging_setting)
        self.logger = logging.getLogger(logger_name)

        self.logger.info('正在初始化mongodb')
        self.mongo_db_get = db
        self.mongo_collection_get = collection
        mongo_host_get = mongodb_setting['host']
        mongo_port_get = mongodb_setting['port']
        self.mongo_conn_get = pymongo.MongoClient(host=mongo_host_get, port=mongo_port_get)
        self.mongo_get = self.mongo_conn_get[self.mongo_db_get][self.mongo_collection_get]

        mongo_host_put = mongodb_setting['host']
        mongo_port_put = mongodb_setting['port']
        self.mongo_db_put = self.mongo_db_get
        self.mongo_collection_put = self.mongo_collection_get + '_most'
        self.mongo_conn_put = pymongo.MongoClient(host=mongo_host_put, port=mongo_port_put)
        self.mongo_put = self.mongo_conn_put[self.mongo_db_put][self.mongo_collection_put]
        self.logger.info('mongodb初始化成功')

        self.anchor_num = high_follow_anchor_num  # 获取共同关注最多的主播的数量
        self.nick_name_dict = dict()  # key是主播名字，value是共同关注的数量
        self.result_list = []  # 存储共同关注量前nick_name_length主播和共同关注量
        self.find_responses = []  # 存储搜索主播的response, 主播名，共同关注量
        self.room_id_list = []  # 存放room_id, 主播名，共同关注量，用于存储和输出
        self.doc_num = self.mongo_get.find({}).count()  # 需要处理的数据数
        self._id = self.mongo_put.find({}).count()  # 数据写入id
        self.session = aiohttp.ClientSession()  # 用于url请求的session

    def get_fan_info(self, index):
        """
        从数据库中获取一个用户的关注列表json数据
        :param index: 数据id
        :return:
        """
        try:
            data = self.mongo_get.find_one({'_id': index})
        except Exception:
            raise Exception
        return data

    def analyse_info(self, info):
        """
        解析一个用户的关注列表json数据，并修改nick_name_dict
        :param info: 关注列表json数据
        :return:
        """
        data = info['follow']
        nick_name_list = [data_line['nickname'] for data_line in data]

        for nick_name in nick_name_list:  # 遍历一个用户的关注列表,如果nick_name_dict存在关注主播，value加1，否则加入该主播，value设置为1
            try:
                self.nick_name_dict[nick_name] += 1
            except Exception:
                self.nick_name_dict[nick_name] = 1

    def set_result_list(self):
        """
        把nick_name_dict转换为列表并排序，取前nick_name_length个赋值给result_list
        :return:
        """
        result = list(zip(self.nick_name_dict.keys(), self.nick_name_dict.values()))
        result.sort(key=lambda x: -x[1])
        result = result[:self.anchor_num]
        self.result_list = result

    async def request_for_room_id(self):
        """
        从result_list中获取主播名，生成搜索主播url，访问url，并将response放入find_responses中
        :return:
        """
        data = self.result_list.pop()
        nick_name = data[0]
        follow_num = data[1]
        url = 'https://www.douyu.com/search/?kw={}'.format(nick_name)
        self.logger.info(url)
        async with self.session.get(url) as repr:
            self.find_responses.append([await repr.text(), nick_name, follow_num])

    async def parse_response(self):
        """
        从find_responses中获取response，从response中解析出主播的房间号
        :return:
        """
        while True:
            try:
                data = self.find_responses.pop()
                break
            except Exception:
                await asyncio.sleep(0.05)
                continue
        html = data[0]
        nick_name = data[1]
        follow_num = data[2]
        xpath_html = etree.HTML(html)
        try:
            room_id = xpath_html.xpath('//div[@class="anchor-action-btn"]/@data-room_id')[0]
        except Exception:  # 主播直播间被封会出现解析不到的情况
            self.logger.error('主播直播间被封')
            self.room_id_list.append(None)
            return
        self.room_id_list.append([room_id, nick_name, follow_num])

    async def save_room_id(self):
        """
        保存房间号，主播名字，关注数
        :return:
        """
        while True:
            try:
                data = self.room_id_list.pop()
                break
            except Exception:
                await asyncio.sleep(0.05)
                continue
        if data is None:
            return
        _room_id = data[0]
        if _room_id == str(room_id):
            return
        nick_name = data[1]
        follow_num = data[2]
        data = {
            '_id': self._id,
            'room_id': _room_id,
            'nick_name': nick_name,
            'follow_num': follow_num
        }
        self._id += 1
        self.logger.info(data)
        self.mongo_put.insert(data)

    def run(self):
        for i in range(self.doc_num):
            data = self.get_fan_info(i)
            self.analyse_info(data)
        self.set_result_list()
        tasks = []
        for _ in range(self.anchor_num):
            tasks.append(asyncio.ensure_future(self.request_for_room_id()))
            tasks.append(asyncio.ensure_future(self.parse_response()))
            tasks.append(asyncio.ensure_future(self.save_room_id()))
        return tasks

    async def close(self):
        self.mongo_conn_get.close()
        self.mongo_conn_put.close()
        await self.session.close()


if __name__ == '__main__':
    start_time = time.time()
    info_db = 'test'
    info_collection = 'douyu252140金咕咕金咕咕doinb_fans_follow'
    loop = asyncio.get_event_loop()
    afi = GetDouyuFansFollowMost(db=info_db, collection=info_collection)
    tasks = afi.run()
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.run_until_complete(afi.close())
    print('use time:{}'.format(time.time()-start_time))



