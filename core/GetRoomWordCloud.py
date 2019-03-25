import logging.config
import pymongo
from wordcloud import WordCloud
from conf.setttings import mongodb_setting, logging_setting, logger_name, word_cloud_setting, room_word_cloud_path


class GetRoomWordCloud:
    def __init__(self, db, collection):
        self.save_path = room_word_cloud_path + '.png'
        logging.config.dictConfig(logging_setting)  #
        self.logger = logging.getLogger(logger_name)

        self.logger.info('正在初始化mongodb')
        self.db_name = db
        self.collection = collection
        self.mongo_conn = pymongo.MongoClient(host=mongodb_setting['host'], port=mongodb_setting['port'])
        self.mongo = self.mongo_conn[self.db_name][self.collection]
        self.logger.info('mongodb初始化完成')
        self.result_str = ''

    def get_barrage(self):
        """
        获取所有的弹幕，存入一个字符串
        :return:
        """
        self.logger.info('正在获取弹幕')
        for word in self.mongo.find({}):
            self.result_str += word['txt']
        self.logger.info('弹幕获取完成')

    def get_img(self):
        """
        生成词云
        :return:
        """
        self.logger.info('正在生成词云')
        wordcloud = WordCloud(**word_cloud_setting).generate(self.result_str)
        img = wordcloud.to_image()
        self.logger.info('生成词云成功')
        img.show()
        self.logger.info('正在保存词云')
        img.save(self.save_path)
        self.logger.info('保存词云成功：{}'.format(self.save_path))

    def run(self):
        self.get_barrage()
        self.get_img()
        self.mongo_conn.close()


if __name__ == '__main__':
    gec = GetRoomWordCloud(db='test', collection='douyu252140金咕咕金咕咕doinb')
    gec.run()