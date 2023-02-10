import json
import os
import time

import redis as redis
import logging
from Scrape import try_tweet_by_id_scrap
import json
import threading


class Scrape(threading.Thread):
    def __init__(self, task):
        super().__init__()
        self.task = task

    def run(self) -> None:
        try_tweet_by_id_scrap(task['id'], task['save_dir'])


if __name__ == '__main__':
    # 检查日志文件夹建立没有
    if not os.path.exists("./logs"):
        os.mkdir("./logs")
    # 设置日志相关内容
    lg = logging.getLogger(f"runner")
    logging.basicConfig(filename=f"./logs/runner.txt", filemode='a', level=logging.INFO,
                        format="%(levelname)s %(asctime)s %(funcName)s %(lineno)d %(message)s")
    # 获取redis客户端
    client = redis.Redis(host=os.environ.get("MYSERVER"), port=6379, password=os.environ.get("REDISCLI_AUTH"))
    while True:
        # 如果没什么内容，那就休息一下
        if client.llen("id_scrape_tasks") == 0:
            lg.log(logging.DEBUG, f"No job to do, sleeping")
            time.sleep(30)
        else:
            task = json.loads(client.lpop("id_scrape_tasks"))
            count = int(client.get(task['date_str'])) - 1
            client.set(task['date_str'], count)
            if not os.path.exists(task['save_dir']):
                lg.log(logging.INFO,
                       f"file {os.path.join(task['save_dir'], task['date_str']) + '.jl'} doesn't exist, creating")
                f = open(task['save_dir'], 'w')
                f.close()
            try:
                t = Scrape(task)
                t.start()
                t.join(2)
                if not t.is_alive():
                    lg.log(logging.INFO,
                       f"Successfully scraped {task['id']} in day {task['date_str']}, {count} tweets remaining")
                else:
                    lg.log(logging.INFO, f"unsuccessful scrape on twitter id: {task['id']}, abort")
                    client.rpush("id_scrape_tasks", json.dumps(task))
                    count += 1
                    client.set(task['date_str'], count)
            except Exception as ex:
                lg.log(logging.WARN, f"An error has took place{ex}, when scraping {task['id']}")
                client.rpush("id_scrape_tasks", json.dumps(task))
                count += 1
                client.set(task['date_str'], count)
                time.sleep(5)
