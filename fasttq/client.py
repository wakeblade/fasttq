#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 12:14:34
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
client.py -- 访问保存任务主题、任务处理器和任务队列的消息队列的客户端
"""

from abc import ABC, abstractmethod
from typing import Type, Callable, List, Dict, Union
from urllib import parse
from redis import Redis, ConnectionPool
import importlib
import json

def func2str(func:Union[Callable, Type]):
    if callable(func):
        return f"{func.__module__}.{func.__name__}"
    
    return f"{func.__module__}.{func.__class__.__name__}"

def str2func(s:str):
    if len(s)<1:
        return None
    
    m, _, f = s.rpartition(".")
    module = importlib.import_module(m)
    return getattr(module, f)

def serialize(data:object, retrys:int = 1, retry_delay:float = 0.01):
    d = dict(
        data=data if isinstance(data, str) else data.__dict__,
        retrys=retrys,
        retry_delay=retry_delay
    )
    return json.dumps(d)

def unserialize(data:str):
    s = str(data, encoding="utf8")
    return json.loads(s) if s.startswith("{") else s

class Client(ABC):

    default_topics_header = "fasttq:topics"
    default_workers_header = "fasttq:workers"
    default_handlers_header = "fasttq:handlers"
    default_jobs_header = "fasttq:jobs:%s"

    conn_url:str = None

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def register(self, topic:str, handle:Callable, before:Callable, after:Callable, priority:int = 0):
        pass

    @abstractmethod
    def unregister(self, topic:str):
        pass

    @abstractmethod
    def push_topic(self, topic:str, jobs:List[str]):
        pass

    @abstractmethod
    def push_topics(self, jobs:Dict[str, str]):
        pass

    @abstractmethod
    def insert_topic(self, topic:str, jobs:List[str]):
        pass

    @abstractmethod
    def insert_topics(self, jobs:Dict[str, str]):
        pass

    @abstractmethod
    def get_topics(self):
        pass

    @abstractmethod
    def get_handlers(self, topic:str):
        pass

    @abstractmethod
    def get_job(self, topic:str):
        pass

    @abstractmethod
    def get_jobs(self, topic:str, chunksize:int = 1):
        pass

class RedisClient(Client):

    def __init__(self, conn_url:str):
        self.conn_url = conn_url
        self.pool = ConnectionPool.from_url(conn_url)

    def connect(self):
        return Redis(connection_pool=self.pool)

    # 注册Topic以及处理器
    def register(self, topic:str, handle:Callable, before:Callable, after:Callable, priority:int = 0):
        with self.connect() as conn:
            t = conn.zadd(self.default_topics_header, {topic:priority,})
            handle_str = func2str(handle)
            before_str = func2str(before) if before else ""
            after_str = func2str(after) if after else ""
            h = conn.hset(self.default_handlers_header, key=topic, value=f"('{handle_str}', '{before_str}', '{after_str}')")
            return t,h

    # 注销Topic以及处理器
    def unregister(self, topic:str):
        with self.connect() as conn:
            w = conn.zrem(self.default_workers_header, topic)
            t = conn.zrem(self.default_topics_header, topic)
            h = conn.hdel(self.default_handlers_header, topic)
            j = conn.delete(self.default_jobs_header % topic)
            return w,t,h,j

    # 推入某个topic的任务
    def push_topic(self, topic:str, jobs:List[str]):
        with self.connect() as conn:
            return conn.lpush(self.default_jobs_header % topic, *jobs)

    # 推入多个topic的任务
    def push_topics(self, jobs:Dict[str, str]):
        with self.connect() as conn:
            return {topic:conn.lpush(self.default_jobs_header % topic, *_jobs) for topic,_jobs in jobs.items()}

    # 优先插入某个topic的任务
    def insert_topic(self, topic:str, jobs:List[str]):
        with self.connect() as conn:
            return conn.rpush(self.default_jobs_header % topic, *jobs)

    # 优先插入多个topic的任务
    def insert_topics(self, jobs:Dict[str, str]):
        with self.connect() as conn:
            return {topic:conn.rpush(self.default_jobs_header % topic, *_jobs) for topic,_jobs in jobs.items()}
            
    # 获取所有活跃的topic
    def get_topics(self):
        with self.connect() as conn:
            return (key[len(self.default_jobs_header)-2:] for key in conn.keys(pattern=self.default_jobs_header % "*"))
            # count = conn.zcard(self.default_topics_header)
            # return conn.zrange(self.default_topics_header, 0 , count, withscores=True).extend(topics)

    # 获取某个topic的处理器
    def get_handlers(self, topic:str):
        with self.connect() as conn:
            return conn.hget(self.default_handlers_header, topic)

    # 获取某个topic的待处理任务
    def get_job(self, topic:str):
        with self.connect() as conn:
            return conn.rpop(self.default_jobs_header % unserialize(topic))

    # 获取某个topic的待处理任务
    def get_jobs(self, topic:str, chunksize:int = 10):
        with self.connect() as conn:
            header = self.default_jobs_header % unserialize(topic)
            return [conn.rpop(header) for _ in range(chunksize)]

    # worker报到
    def report(self, topic:str, worker:str):
        with self.connect() as conn:
            return conn.hset(self.default_workers_header, key=topic, value=worker)

    # worker告退
    def unreport(self, topic:str):
        with self.connect() as conn:
            return conn.hdel(self.default_workers_header, key=topic)

    # 获取有报到的所有topic
    def get_topics_reported(self):
        with self.connect() as conn:
            return conn.hkeys(self.default_workers_header)
