#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 21:17:58
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
simple.py -- 
"""

import requests

from fastq import FastQueue, RedisClient

client = RedisClient.create("redis://localhost:6379/0")
fastq = FastQueue(client)

@fastq.register(topic="fetch_url")
def fetch_url(url:str, *args):
    res = requests.get(url)
    print(url)
    print("#"*80)
    print(res.text)
    return res.text

@fastq.topic(topic="fetch_url")
def push_urls():
    return [
        "http://www.baidu.com",
        "http://www.bing.com"
    ]

if __name__ == "__main__":
    fastq.start_workers(4, retry_delay=0.01)