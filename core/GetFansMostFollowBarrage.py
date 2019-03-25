import logging.config
import pymongo
from conf.setttings import mongodb_setting, logging_setting, logger_name, follow_barrage_num
from core.GetDouyuBarrage import GetDouyuBarrage, RoomError
from concurrent.futures import ThreadPoolExecutor



class GetFansMostFollowBarrage:
    def __init__(self, db, collection):
        logging.config.dictConfig(logging_setting)  #
        self.logger = logging.getLogger(logger_name)
        self.room_id_list = []  # 用于存放共同关注最多的房间号

        self.logger.info('正在初始化mongodb')
        self.db_name = db
        self.collection = collection
        self.mongo_conn = pymongo.MongoClient(host=mongodb_setting['host'], port=mongodb_setting['port'])
        self.mongo = self.mongo_conn[self.db_name][self.collection]
        self.logger.info('mongodb初始化完成')

    def get_room_id_list(self):
        self.logger.info('正在获取房间号列表')
        datas = self.mongo.find({})
        for data in datas:
            self.room_id_list.append(data['room_id'])
        self.logger.info('获取房间号列表完成')

    def thread_get_barrage(self, room_id):
        try:
            gdb = GetDouyuBarrage(room_id=room_id, barrage_num=follow_barrage_num)
        except RoomError as e:
            self.logger.error(e)
            return
        except Exception as e:
            self.logger.error(e)
            return
        gdb.run()

    def run(self, thread_pool):
        self.get_room_id_list()
        self.logger.info('正在获取弹幕')
        for room_id in self.room_id_list:
            thread_pool.submit(self.thread_get_barrage, room_id)
        thread_pool.shutdown()
        self.logger.info('获取弹幕完成')
        self.mongo_conn.close()


if __name__ == '__main__':
    gec = GetFansMostFollowBarrage(db='test', collection='douyu252140金咕咕金咕咕doinb_fans_follow_most')
    t_pool = ThreadPoolExecutor()
    gec.run(t_pool)