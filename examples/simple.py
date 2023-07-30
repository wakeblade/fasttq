#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 21:17:58
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
simple.py -- 简单示例：采用注解方式推送任务
"""

import requests
import sys
sys.path.append("..")

from fasttq import FastQueue, RedisClient

client = RedisClient("redis://localhost:6379/0")
fq = FastQueue(client)

@fq.register(topic="fetch_url")
def fetch_url(url:str, *args):
    res = requests.get(url)
    print(url)
    print("#"*80)
    print(res.text)
    return res.text

@fq.topic(topic="fetch_url")
def push_urls():
    return [
        "http://www.baidu.com",
        "http://www.bing.com"
    ]

@fq.topics()
def push_urls():
    return {
        "fetch_url":[
            "http://www.baidu.com",
            "http://www.bing.com"
        ]
    }

if __name__ == "__main__":
    fq.start(4, retry_delay=0.01)