import time

from Scrape import try_tweet_by_id_scrap
from Collect import get_original_data, extract
import csv
import logging


if __name__ == '__main__':
    gz = get_original_data(2022, 12, 2)
    src_file = extract(gz)
    with open(src_file, "r") as f:
        reader = csv.reader(f, delimiter='\t')
        collected = 0
        for line in reader:
            twitter_id = line[0]
            language = line[3]
            if language == 'en':
                try:
                    try_tweet_by_id_scrap(twitter_id)
                    collected += 1
                except RuntimeError:
                    print("发生异常")
                    time.sleep(30)
        print(f"{collected} tweets has been collected")
