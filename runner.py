import json
import os

import redis as redis


client = redis.Redis(host=os.environ.get("MYSERVER"), port=6379, password=os.environ.get("REDISCLI_AUTH"))
ls = client.llen("tasks")
client.delete("tasks")
print(ls)
