#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 16:23:29
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
worker.py -- 任务处理子进程
"""

from enum import Enum
import time
from collections import defaultdict
from functools import reduce
import os

from .client import Client, str2func, unserialize

class Assignor(Enum):
    PriorityOne = 0 # 按优先级广度遍历
    PriorityAll = 1 # 按优先级深度遍历
    RoundRobinOne = 2 # 轮询广度遍历
    RoundRobinAll = 3 # 轮询深度遍历

class Pusher:
    
    def __init__(self, client_str:str, conn_url:str):
        client_class = str2func(client_str)
        self.client:Client = client_class.create(conn_url)

    def push_topic(self, topic:str, jobs:list):
        print(f"Pusher({os.getpid()}): {len(jobs)}", "#"*40)
        return self.client.push_topic(topic, jobs)

    def push_topics(self, jobs:dict):
        print(f"Pusher({os.getpid()}): {len(jobs)}", "#"*40)
        return self.client.push_topics(jobs)

class Worker:

    _handlers = {}
    _jobs = defaultdict(list)
    _stop = False 

    def __init__(self, client_str:str, conn_url:str, assignor:Assignor = Assignor.PriorityOne, chunksize:int = 1, retrys:int = 10, retry_delay:int = 1):
        client_class = str2func(client_str)
        self.client:Client = client_class.create(conn_url)
        self.assignor = assignor
        self.chunksize = chunksize 
        self.retrys = retrys
        self.retry_delay = retry_delay

    def len(self):
        if len(self._jobs)<1:
            return 0
        
        if len(self._jobs)<2:
            return [len(self._jobs[k]) for k in self._jobs][0]

        return reduce(lambda x,y:x+y, [len(self._jobs[k]) for k in self._jobs])

    def load_handlers(self, topic:str):
        if topic not in self._handlers:
            handlers = eval(self.client.get_handlers(topic))
            self._handlers[topic] = tuple(str2func(handle) for handle in handlers)
        return self._handlers[topic]

    def get_jobs(self, topic:str):
        retry_times = 0
        while retry_times<self.retrys:
            jobs = self.client.get_jobs(topic, self.chunksize)
            if jobs is not None and len(jobs)>0:
                return jobs
            retry_times+=1
            time.sleep(self.retry_delay)
        return None

    def get_job(self, topic:str):
        retry_times = 0
        while retry_times<self.retrys:
            job = self.client.get_job(topic)
            if job is not None and len(job)>0:
                return job
            retry_times+=1
            time.sleep(self.retry_delay)
        return None

    def load_jobs(self):
        topics = self.client.get_topics()
        last_priority = -1
        while self.len()<self.chunksize: 
            last_len = self.len()
            for topic, priority in topics:
                if (self.assignor.value%2)==1:
                    jobs = self.get_jobs(topic)
                    if jobs is not None:
                        for job in jobs:
                            self._jobs[topic].append(unserialize(job))
                else:
                    job = self.get_job(topic)
                    if job is not None:
                        self._jobs[topic].append(unserialize(job))
                
                if last_priority==priority or last_priority<1:
                    continue
            
            if last_len == self.len():
                break

        return self._jobs

    def work(self):
        _retry_times = 0
        while not self._stop and _retry_times<self.retrys:
            _jobs = self.load_jobs()
            if len(_jobs)<1:
                break

            topic, jobs = _jobs.popitem()
            if len(jobs)<1:
                time.sleep(self.retry_delay)
                _retry_times +=1
                print(f"Workser({os.getpid()}): _retry_times = {_retry_times}", "#"*40)
            else:
                _retry_times = 0

            handle, before, after = self.load_handlers(topic)
            _result = []
            context = before(topic) if before else None
            for job in jobs:
                data, retrys, retry_delay = job["data"], int(job["retrys"]), float(job["retry_delay"])
                retry_times = 0
                while retry_times<retrys:
                    try:
                        rs = handle(data, context)
                        _result.append(rs)
                        break
                    except:
                        retry_times +=1
                        time.sleep(retry_delay)
                        print(f"Job({os.getpid()}): _retry_times = {_retry_times}", "#"*40)
                        continue
            after(data, context, _result) if after else None
            print(f"Workser({os.getpid()}): {len(_result)}", "#"*40)
        