import time

from Scrape import try_tweet_by_id_scrap
from Collect import get_original_data, extract
import csv
import logging
import argparse

parse = argparse.ArgumentParser(description="Scraping all covid related tweet in a day")
parse.add_argument("--year", type=int)
parse.add_argument("--month", type=int)
parse.add_argument("--day", type=int)


if __name__ == '__main__':
    args = parse.parse_args()
    gz = get_original_data(args.year, args.month, args.day)
    src_file = extract(gz)
    # 构建任务索引
    index = []
    with open(src_file, "r") as f:
        reader = csv.reader(f, delimiter='\t')
        collected = 0
        for line in reader:
            twitter_id = line[0]
            language = line[3]
            if language == 'en':
                index.append(twitter_id)
        print(f"There are {len(index)} tweets in {args.year}-{args.month}-{args.day} to be scraped")

    for idx, item in enumerate(index):
        result = try_tweet_by_id_scrap(item)
        print(f"{idx}: tweet number {item} downloaded")
