#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 21:17:58
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
mp.py -- 采用注解方式多进程推送任务
"""

import requests
from pathlib import Path
import sys
sys.path.append("..")

from fasttq import FastQueue, RedisClient

client = RedisClient.create("redis://localhost:6379/0")
fq = FastQueue(client)

@fq.register(topic="pathParse")
def parse(path:str, *args):
    # print(Path(path).parts)
    return Path(path).parts

@fq.jobs(topic="pathParse")
def scan(topic:str, *args):
    path = r"D:\data\FUTURES\1m"
    return (str(p) for p in Path(path).rglob("*.*"))

if __name__ == "__main__":
    # fasttq.pending(7, retry_delay=0.01)
    fq.start_mp(7, retry_delay=0.01)