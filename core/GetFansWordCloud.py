import logging.config
import pymongo
from wordcloud import WordCloud
from conf.setttings import mongodb_setting, logging_setting, logger_name, word_cloud_setting, fans_word_cloud_path


class GetFansWordCloud:
    def __init__(self, db, collection):
        self.save_path = fans_word_cloud_path + '.png'
        logging.config.dictConfig(logging_setting)  #
        self.logger = logging.getLogger(logger_name)

        self.logger.info('正在初始化mongodb')
        self.db_name = db
        self.collection = collection
        self.mongo_conn = pymongo.MongoClient(host=mongodb_setting['host'], port=mongodb_setting['port'])
        self.mongo = self.mongo_conn[self.db_name][self.collection]
        self.logger.info('mongodb初始化完成')
        self.result_str = ''

    def get_mongo(self, nick_name, room_id):
        """
        更具nick_name和room_id生成表名，获取表并返回
        :param nick_name:
        :param room_id:
        :return:
        """
        collection_name = 'douyu{}{}'.format(room_id, nick_name)
        mongo = self.mongo_conn[self.db_name][collection_name]
        return mongo

    def get_barrage(self):
        """
        从共同关注最多的主播的表里面获取数据，通过get_mongo获取表
        然后获取所有弹幕，放入result_str中
        :return:
        """
        self.logger.info('正在获取弹幕')
        datas = self.mongo.find({})
        for data in datas:
            mongo = self.get_mongo(data['nick_name'], data['room_id'])
            barrage_datas = mongo.find({})
            for barrage_data in barrage_datas:
                self.result_str += barrage_data['txt']
        self.logger.info('获取弹幕完成')

    def get_img(self):
        """
        通过result_str生成词云并存储
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
    gec = GetFansWordCloud(db='test', collection='douyu252140金咕咕金咕咕doinb_fans_follow_most')
    gec.run()