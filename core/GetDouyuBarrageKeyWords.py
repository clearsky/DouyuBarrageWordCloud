import pymongo
import jieba
import logging.config
from conf.setttings import mongodb_setting,logging_setting, logger_name


class GetDouyuBarrageKeyWords:
    def __init__(self, db, collection):
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
        self.mongo_collection_put = self.mongo_collection_get + '_key_words'
        self.mongo_conn_put = pymongo.MongoClient(host=mongo_host_put, port=mongo_port_put)
        self.mongo_put = self.mongo_conn_put[self.mongo_db_put][self.mongo_collection_put]
        self.logger.info('mongodb初始化成功')

        self.barrage_key_word_num = barrage_key_word_num # 关键词的数量
        self.doc_num = self.mongo_get.find({}).count()  # 需要处理的数据数
        self._id = self.mongo_put.find({}).count  # 写入数据的id
        self.barrage_dict = dict()  # key为弹幕， value为弹幕出现次数
        self.result_list = []  # 存储结果的list
        self._id = self.mongo_put.find({}).count()  # 数据的id

    def get_barrage(self, index):
        """
        从数据库中获取弹幕
        :param index: 数据的id
        :return: 获取的弹幕
        """
        return self.mongo_get.find_one({'_id': index})

    def analyse(self, barrage):
        """
        解析弹幕，进行分词，并修改barrage_dict
        :param barrage:
        :return:
        """
        text = barrage['txt'].strip().replace(' ', '')
        words = jieba.cut(text)  # 对弹幕进行分词处理

        for word in words:
            if len(word) < 2:  # 单词长度小于2的忽略
                continue
            try:
                self.barrage_dict[word] += 1
            except Exception:
                self.barrage_dict[word] = 1

    def set_result_list(self):
        """
        将barrage_dict转换为列表，并进行排序，将排序的结果赋值给result_list
        :return:
        """
        result = list(zip(self.barrage_dict.keys(), self.barrage_dict.values()))
        result.sort(key=lambda x: -x[1])
        self.result_list = result[:self.barrage_key_word_num]

    def save_data(self):
        """
        将结果写入数据库
        :return:
        """
        self.logger.info(self.result_list)
        for index, line in enumerate(self.result_list):
            self.result_list[index] = {'_id': self._id, 'word': line[0], 'num': line[1]}
            self._id += 1
        self.logger.info(self.result_list)
        self.mongo_put.insert(self.result_list)  # 将数据写入数据库

    def close(self):
        """
        关闭数据库连接
        :return:
        """
        self.mongo_conn_put.close()
        self.mongo_conn_get.close()

    def run(self):
        for i in range(self.doc_num):
            danmu = self.get_barrage(i)
            self.analyse(danmu)
        self.set_result_list()
        self.save_data()
        self.close()


if __name__ == '__main__':
    ald = GetDouyuBarrageKeyWords(db='test', collection='douyu252140金咕咕金咕咕doinb')
    ald.run()
