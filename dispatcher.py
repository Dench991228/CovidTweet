import os
import time

from Scrape import try_tweet_by_id_scrap
from Collect import get_original_data, extract
import csv
import logging
import argparse
import redis
import json

parse = argparse.ArgumentParser(description="Scraping all covid related tweet in a day")
parse.add_argument("--year", type=int, required=True)
parse.add_argument("--month", type=int, required=True)
parse.add_argument("--day", type=int, required=True)
parse.add_argument("--save", type=str, required=True, default="./results")


def get_date_str(y, m, d):
    result = str(y)+"-"
    result += ("0"+str(m) if m < 10 else str(m))
    result += '-'
    result += ("0"+str(d) if d < 10 else str(d))
    return result


if __name__ == '__main__':
    args = parse.parse_args()
    print("Downloading index file from github")
    gz = get_original_data(args.year, args.month, args.day)
    print("Extracting file")
    src_file = extract(gz)
    # 检查日志文件夹建立没有
    if not os.path.exists("./logs"):
        os.mkdir("./logs")
    # 检查sources文件夹建立没有
    if not os.path.exists("./sources"):
        os.mkdir("./sources")
    # 日期字符串
    date_str = get_date_str(args.year, args.month, args.day)
    # 获取日志
    logging.basicConfig(filename=f"./logs/{date_str}.txt", filemode='a')
    # 目标目录
    data_dir = args.save
    # 创建redis连接
    client = redis.Redis(host=os.environ.get("MYSERVER"), port=6379, password=os.environ.get("REDISCLI_AUTH"))
    logging.log(logging.INFO, f"Start dispatching scraping jobs of {date_str}")
    with open(src_file, "r") as f:
        reader = csv.reader(f, delimiter='\t')
        collected = 0
        length = 0
        for line in reader:
            twitter_id = line[0]
            language = line[3]
            if language == 'en':
                # 被插入的工作
                task = {
                    "id": twitter_id,
                    "date_str": date_str,
                    "save_dir": args.save
                }
                # 将目标工作插入到待完成的列表中
                # client.lpush("tasks", json.dumps(task))
                length += 1
        logging.log(logging.INFO, f"There are {length} tweets in {args.year}-{args.month}-{args.day} to be scraped")