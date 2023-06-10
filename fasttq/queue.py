#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 15:50:12
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
queue.py -- 任务处理器注册和任务注册的装饰器，以及启动任意多个Worker子进程
"""

from typing import Callable, Dict, List, Union
from multiprocessing import Process, Pool, cpu_count
from urllib import parse
from functools import partial
import psutil
import os
import signal
import platform
if platform.system()=='Windows':
    def oskill(pid:int, signal:signal):
        os.popen(f"taskkill /pid:{pid}")
else:
    def oskill(pid:int, signal:signal):
        os.kill(pid, signal)

from .client import Client, serialize, func2str
from .worker import Pusher, Worker, Assignor

def items2chunk(items:list, chunksize:int = 100):
    return (items[i*chunksize:(i+1)*chunksize] for i in range(len(items)//chunksize+1))

def items2dict(items:list):
    return {k:v for k,v in items}

def dict2chunk(d:dict, chunksize:int = 100):
    return [items2dict(items) for items in items2chunk(d.items(), chunksize)]

def start_worker(client_str:str, conn_url:str, assignor:Assignor = Assignor.PriorityOne, chunksize:int = 0, retrys:int = 10, retry_delay:int = 1):
    worker = Worker(client_str, conn_url, assignor, chunksize, retrys, retry_delay)
    worker.work()

def start_pusher(client_str:str, conn_url:str, topic:str = None, jobs:Union[List, Dict] = None):
    pusher = Pusher(client_str, conn_url)
    if topic is None:
        pusher.push_topics(jobs)
    else:
        pusher.push_topic(topic, jobs)

class FastQueue:

    _workers:Dict[str, Process] = {}

    def __init__(self, client:Client):
        self.client = client
        self._stop = False
        self.getJobs = []

    def register(self, topic:str, before:Callable = None, after:Callable = None):
        def decorator(handle:Callable):
            self.client.register(topic, handle, before, after)
            return handle
        return decorator        

    def jobs(self, topic:str = None):
        def decorator(func:Callable):
            self.getJobs.append(partial(func, topic))
            return self.getJobs
        return decorator        

    def pending(self, retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 100, **kwargs):
        for getJob in self.getJobs:
            topic = getJob.args[0]
            if topic is None:
                jobs = {topic:serialize(data, retrys, retry_delay) for topic, data in getJob(**kwargs).items()}
                for chunk in dict2chunk(jobs, chunksize):
                    self.client.push_topics(chunk)
            else:
                jobs = [serialize(data, retrys, retry_delay) for data in getJob(**kwargs)]
                for chunk in items2chunk(jobs, chunksize):
                    self.client.push_topic(topic, chunk)

    #########################################################################################
    # 方案3.0，替换成Pool模式
    def pending_mp(self, client_str:str, conn_url:str, retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 100, **kwargs):
        for getJob in self.getJobs:
            topic = getJob.args[0]
            if topic is None:
                jobs = {topic:serialize(data, retrys, retry_delay) for topic, data in getJob(**kwargs).items()}
                chunks = dict2chunk(jobs, chunksize)
            else:
                jobs = [serialize(data, retrys, retry_delay) for data in getJob(**kwargs)]
                chunks = items2chunk(jobs, chunksize)
            with Pool(processes=cpu_count()) as pool:
                for chunk in chunks:
                    pool.apply_async(
                        start_pusher,
                        (client_str, conn_url, topic, chunk)
                    )
                pool.close()
                pool.join()

    #########################################################################################
    # 方案2.0，可以任务数据函数用装饰器注入self.getJobs
    # def start_pusher(self, client_str:str, conn_url:str, topic:str = None, jobs:Union[List, Dict] = None, daemon:bool = True):
    #     process = Process(
    #         target=start_pusher, 
    #         args=(client_str, conn_url, topic, jobs))
    #     process.daemon = daemon
    #     process.start()
    #     print("#"*80)
    #     return process

    # def pending_mp(self, client_str:str, conn_url:str, retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 100):
    #     if self.getJobs is not None:
    #         topic = self.getJobs.args[0]
    #         if topic is None:
    #             jobs = {topic:serialize(data, retrys, retry_delay) for topic, data in self.getJobs().items()}
    #             for chunk in dict2chunk(jobs, chunksize):
    #                 process = self.start_pusher(client_str, conn_url, None, chunk, daemon=False)
    #                 process.join()
    #         else:
    #             jobs = [serialize(data, retrys, retry_delay) for data in self.getJobs()]
    #             for chunk in items2chunk(jobs):
    #                 process = self.start_pusher(client_str, conn_url, topic, chunk, daemon=False)
    #                 process.join()

    #########################################################################################
    # 方案1.0，只能把任务数据作为参数注入
    # def topic_mp(self, topic:str, datas:list, retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 100):
    #     client_str = func2str(self.client)
    #     conn_url = self.client.conn_params.geturl()
    #     jobs = [serialize(data, retrys, retry_delay) for data in datas]
    #     for chunk in items2chunk(jobs):
    #         process = self.start_pusher(client_str, conn_url, topic, chunk, daemon=False)
    #         process.join()

    # def topics_mp(self, datas:Dict[str, List], retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 1):
    #     client_str = func2str(self.client)
    #     conn_url = self.client.conn_params.geturl()
    #     d = {topic:serialize(data, retrys, retry_delay) for topic, data in datas.items()}
    #     for chunk in dict2chunk(d, chunksize):
    #         process = self.start_pusher(client_str, conn_url, None, chunk, daemon=False)
    #         process.join()

    def topic(self, topic:str, retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 100):
        def decorator(func:Callable):
            jobs = [serialize(data, retrys, retry_delay) for data in func()]
            for chunk in items2chunk(jobs, chunksize):
                self.client.push_topic(topic, chunk)
        return decorator

    def topics(self, retrys:int = 1, retry_delay:float = 1.0, chunksize:int = 100):
        def decorator(func:Callable):
            jobs = {topic:serialize(data, retrys, retry_delay) for topic, data in func().items()}
            for chunk in dict2chunk(jobs, chunksize):
                self.client.push_topics(chunk)
        return decorator

    def clear_worker(self):
        # 收集死进程
        _tobekill = []
        for pid, process in self._workers.items():
            if not process.is_alive():
                _tobekill.append(pid)

        if len(self._workers)==len(_tobekill):
            self._stop = True

        # 清理死进程
        for pid in _tobekill:
            self._workers.pop(pid)
            # os.kill(pid, signal.CTRL_C_EVENT)
            if psutil.pid_exists(pid):
                # print(f"Workser({pid}): 销毁成功！", "#"*40)
                oskill(pid, signal.SIGBREAK)
        
        return len(_tobekill) + len(self._workers)
        
    def start_worker(self, client_str:str, conn_url:str, assignor:Assignor = Assignor.PriorityOne, chunksize:int = 100, retrys:int = 10, retry_delay:int = 1, daemon:bool = True):
        process = Process(
            target=start_worker, 
            args=(client_str, conn_url, assignor, chunksize, retrys, retry_delay))
        process.daemon = daemon
        process.start()
        return process

    def start_workers(self, client_str:str, conn_url:str, workers:int = 1, assignor:Assignor = Assignor.PriorityOne, chunksize:int = 100, retrys:int = 10, retry_delay:int = 1):
        # 补充新进程
        while not self._stop:
            while(workers-1>len(self._workers)):
                process = self.start_worker(client_str, conn_url, assignor, chunksize, retrys, retry_delay)
                self._workers[process.pid] = process
                # print(f"Workser({process.pid}): 创建成功({workers}/{len(self._workers)})！", "#"*40)
            workers = self.clear_worker()
        # for pid, process in self._workers.items():
        #     process.close()
        #     process.join()

    def start(self, workers:int = 1, assignor:Assignor = Assignor.PriorityOne, chunksize:int = 100, retrys:int = 10, retry_delay:int = 1, **kwargs):
        client_str = func2str(self.client)
        self.pending(*args, retrys, retry_delay, **kwargs)
        self.start_workers(client_str, self.client.conn_url, workers, assignor, chunksize, retrys, retry_delay)
        
    def start_mp(self, workers:int = 1, assignor:Assignor = Assignor.PriorityOne, chunksize:int = 100, retrys:int = 10, retry_delay:int = 1, **kwargs):
        client_str = func2str(self.client)
        self.pending_mp(client_str, self.client.conn_url, retrys, retry_delay, chunksize, **kwargs)
        self.start_workers(client_str, self.client.conn_url, workers, assignor, chunksize, retrys, retry_delay)
