import pymongo
import asyncio
import aiohttp
import json
import time
import logging.config
from conf.setttings import mongodb_setting, douyu_setting, async_sleep, get_follows_number, logging_setting, logger_name


class GetDouyuFansFollow:
    """
    从mongodb中读取GetDouyuBarrage存储的弹幕信息
    通过用户名和用户id获取用户的关注列表并存储到mongodb
    """
    def __init__(self, *args, db, collection):
        logging.config.dictConfig(logging_setting)
        self.logger = logging.getLogger(logger_name)

        self.logger.info('正在初始化mongodb')  # 初始化mongodb
        self.mongo_db_get = db
        self.mongo_collection_get = collection
        mongo_host_get = mongodb_setting['host']
        mongo_port_get = mongodb_setting['port']
        self.mongo_conn_get = pymongo.MongoClient(host=mongo_host_get, port=mongo_port_get)
        self.mongo_get = self.mongo_conn_get[self.mongo_db_get][self.mongo_collection_get]

        mongo_host_put = mongodb_setting['host']
        mongo_port_put = mongodb_setting['port']
        self.mongo_db_put = self.mongo_db_get
        self.mongo_collection_put = self.mongo_collection_get + '_fans_follow'
        self.mongo_conn_put = pymongo.MongoClient(host=mongo_host_put, port=mongo_port_put)
        self.mongo_put = self.mongo_conn_put[self.mongo_db_put][self.mongo_collection_put]
        self.logger.info('mongodb初始化成功')

        self.doc_num = self.mongo_get.find({}).count()  # 设置弹幕条数
        self.session = aiohttp.ClientSession()  # 创建session，用于访问网页
        self.yuba_urls = []  # 存储用户鱼吧跳转地址
        self.yuba_responses = []  # 存储请求用户鱼吧地址返回的response，是真正的鱼吧地址
        self.guanzu_urls = []  # 存储用户关注地址
        self.guanzu_responses = []  # 存储请求关注地址返回的response
        self.resul_list = []  # 存放解析后的数据，用于输出和存储
        self.sleep_time = async_sleep  # 协程中获取数据循环的等待时间
        self._id = self.mongo_put.find({}).count()  # 写入数据的id
        self.seen = set()  # 存储已经处理过的用户id
        cookie = douyu_setting['cookie']  # 访问用户关注列表需要登陆，这里设置登录cookie
        self.header = {
            'Cookie': cookie
        }

    async def produce_yuba_url(self, index):
        """
        从数据库,获取弹幕数据,用用户id和用户名生成用户鱼吧跳转地址，并放入yuba_urls
        :param index: 数据id
        :return:
        """
        try:
            data = self.mongo_get.find_one({'_id': index})  # 从数据库获取数据
        except Exception:
            raise Exception

        try:
            uid = data['uid']  # 用户id
            nn = data['nn']  # 用户名
        except Exception:  # 如果获取uid和nn出现异常，打印从数据库获取的数据
            self.logger.error(data)
            raise Exception

        if uid in self.seen:  # 如果这个用户被处理过，过将None添加到yuba_urls
            self.yuba_urls.append(None)
            return
        else:
            self.seen.add(uid)

        url = 'https://yuba.douyu.com/wbapi/web/jumpusercenter?id={}&name={}'.format(uid, nn)
        self.yuba_urls.append([url, uid, nn])  # 用户名和用户id需要一直传递,以对应用户和关注列表

    async def request_yuba(self):
        """
        从yuba_urls获取用户鱼吧跳转地址，并访问，获取真正的鱼吧地址，放入yuba_respose中
        :return:
        """
        while True:  # 从yaba_urls中取一个地址，如果取不到，就等一会儿再取
            try:
                url = self.yuba_urls.pop()
                break
            except Exception:
                await asyncio.sleep(self.sleep_time)
                continue
        if url is None:
            self.yuba_responses.append(None)
            return
        uid = url[1]
        nn = url[2]
        url = url[0]
        async with self.session.get(url) as reps:
            result_url = str(reps.url)
            self.yuba_responses.append([result_url, uid, nn])

    async def produce_guanzhu_url(self):
        """
        从yuba_responses中获取真正的鱼吧地址，生成用户关注列表地址,并放入guanzhu_urls
        :return:
        """
        while True:  # 从yaba_reponse中取一个地址，如果取不到，就等一会儿再取
            try:
                url = self.yuba_responses.pop()
                break
            except Exception:
                await asyncio.sleep(self.sleep_time)
                continue
        if url is None:
            self.guanzu_urls.append(None)
            return
        uid = url[1]
        nn = url[2]
        url = url[0]
        safe_id = url.split('/')[-1]  # 获取用户的safeid
        result_url = 'https://yuba.douyu.com/wbapi/web/user/followList/{}?limit={}'.format(safe_id, get_follows_number)
        self.logger.info(result_url)

        self.guanzu_urls.append([result_url, uid, nn])

    async def request_guanzhu(self):
        """
        从关注url列表获取关注url，访问url并把返回结果放入guanzhu_responses,获取的response是json格式数据
        :return:
        """
        while True:  # 从guanzhu_urls中取一个地址，如果取不到，就等一会儿再取
            try:
                url = self.guanzu_urls.pop()
                break
            except Exception:
                await asyncio.sleep(self.sleep_time)
                continue
        if url is None:
            self.guanzu_responses.append(None)
            return
        uid = url[1]
        nn = url[2]
        url = url[0]
        async with self.session.get(url, headers=self.header) as reps:  # 请求用户关注列表地址
            data = await reps.text()
            self.guanzu_responses.append([data, uid, nn])

    async def parse_guanzhu(self):
        """
        从guanzhu_responses获取数据，并解析，将结果放入result_list中
        :return:
        """
        while True: # 从guanzhu_reponse中取一个地址，如果取不到，就等一会儿再取
            try:
                data = self.guanzu_responses.pop()
                break
            except Exception:
                await asyncio.sleep(self.sleep_time)
                continue
        if data is None:
            self.resul_list.append(None)
            return
        uid = data[1]
        nn = data[2]
        data = data[0]
        result = json.loads(data)
        if result['status_code'] != 200:  # 如果status_code出错，添加None到result)list
            self.resul_list.append(None)
            return
        result = result['data']['list']
        self.resul_list.append([result, uid, nn])

    async def save_data(self):
        while True:
            try:
                data = self.resul_list.pop()
                break
            except Exception:
                await asyncio.sleep(self.sleep_time)
                continue
        if data is None:
            return
        if data[0]:
            uid = data[1]
            nn = data[2]
            data = data[0]
            data = {'_id': self._id, 'uid': uid, 'nn': nn, 'follow': data}
            self._id += 1
            self.mongo_put.insert(data)

    def run(self):
        tasks = []  # 任务列表
        for i in range(self.doc_num):  # 创建任务列表
            tasks.append(asyncio.ensure_future(self.produce_yuba_url(i)))
            tasks.append(asyncio.ensure_future(self.request_yuba()))
            tasks.append(asyncio.ensure_future(self.produce_guanzhu_url()))
            tasks.append(asyncio.ensure_future(self.request_guanzhu()))
            tasks.append(asyncio.ensure_future(self.parse_guanzhu()))
            tasks.append(asyncio.ensure_future(self.save_data()))
        return tasks

    async def close(self):
        """
        关闭aiohttp的session
        :return:
        """
        self.mongo_conn_get.close()
        self.mongo_conn_put.close()
        await self.session.close()


if __name__ == '__main__':
    start_time = time.time()
    df = GetDouyuFansFollow(db='test', collection='douyu252140金咕咕金咕咕doinb')
    loop = asyncio.get_event_loop()
    tasks = df.run()
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.run_until_complete(df.close())
    print('use time:{}'.format(time.time()-start_time))


