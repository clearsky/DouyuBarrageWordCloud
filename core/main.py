import logging.config
import asyncio
from core.GetDouyuBarrage import GetDouyuBarrage, RoomError
from core.GetDouyuFansFollow import GetDouyuFansFollow
from core.GetDouyuFansFollowMost import GetDouyuFansFollowMost
from core.GetRoomWordCloud import GetRoomWordCloud
from core.GetFansMostFollowBarrage import GetFansMostFollowBarrage
from core.GetFansWordCloud import GetFansWordCloud
from conf.setttings import room_id, barrage_num, logging_setting, logger_name
from concurrent.futures import ThreadPoolExecutor


def main():
    logging.config.dictConfig(logging_setting)
    logger = logging.getLogger(logger_name)
    loop = asyncio.get_event_loop()

    # 获取弹幕
    logger.info('开始获取弹幕,房间号:{}'.format(room_id))
    try:
        gdb = GetDouyuBarrage(room_id, barrage_num=barrage_num)
        gdb.run()
    except RoomError as e:
        logger.error(e)
        return
    logger.info('获取弹幕完成,房间号:{}'.format(room_id))
    # 获取弹幕数据库数据库名和表名
    barrage_db_name = gdb.mongo_db_name
    barrage_collection = gdb.mongo_db_collection

    # 获取关注列表
    logger.info('开始获取关注列表,房间号:{}'.format(room_id))
    gdff = GetDouyuFansFollow(db=barrage_db_name, collection=barrage_collection)
    gdff_tasks = gdff.run()
    loop.run_until_complete(asyncio.gather(*gdff_tasks))
    loop.run_until_complete(gdff.close())
    logger.info('获取关注列表成功,房间号:{}'.format(room_id))

    # 获取关注列表数据库名和表名
    follow_db_name = gdff.mongo_db_put
    follow_collectino = gdff.mongo_collection_put

    # 获取最多关注列表
    logger.info('开始获取最多共同关注列表主播列表,房间号:{}'.format(room_id))
    gdffm = GetDouyuFansFollowMost(db=follow_db_name, collection=follow_collectino)
    gdffm_tasks = gdffm.run()
    loop.run_until_complete(asyncio.gather(*gdffm_tasks))
    loop.run_until_complete(gdffm.close())
    logger.info('获取最多共同关注列表主播列表完成,房间号:{}'.format(room_id))

    # 生成词云
    logger.info('开始生成词云')
    gec = GetRoomWordCloud(db=barrage_db_name, collection=barrage_collection)
    gec.run()
    logger.info('生成词云完成')
    # 生成词云结束

    # 获取共同关注的主播房间的弹幕
    most_follow_db_name = gdffm.mongo_db_put
    most_follow_collection = gdffm.mongo_collection_put
    logger.info('开始获取共同关注房间弹幕')
    gfmfb = GetFansMostFollowBarrage(db=most_follow_db_name, collection=most_follow_collection)
    t_pool = ThreadPoolExecutor()
    gfmfb.run(t_pool)
    logger.info('获取共同关注房间弹幕完成')

    # 生成共同关注房间词云
    logger.info('正在生成共同关注房间词云')
    gfwc = GetFansWordCloud(db=most_follow_db_name, collection=most_follow_collection)
    gfwc.run()
    logger.info('生成共同关注房间词云完成')


