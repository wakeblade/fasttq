#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

# @Time    :2023/05/27 15:50:12
# @Author  :wakeblade (2390245#qq.com) 
# @version :8.1

"""
__init__.py -- 
"""

from typing import Callable, Dict
from multiprocessing import Process
from urllib import parse
from functools import wraps
import os
import signal

from .client import Client, serialize, func2str
from .worker import Worker, Assignor

def start_worker(client_str:str, conn_url:str, assignor:Assignor = Assignor.PriorityOne, batch:int = 0, retrys:int = 10, retry_delay:int = 1):
    worker = Worker(client_str, conn_url, assignor, batch, retrys, retry_delay)
    worker.work()

class FastQueue:

    _workers:Dict[str, Process] = {}

    def __init__(self, client:Client):
        self.client = client
        self._stop = False

    def register(self, topic:str, before:Callable = None, after:Callable = None):
        def decorator(handle:Callable):
            self.client.register(topic, handle, before, after)
            return handle
        return decorator        

    def topic(self, topic:str, retrys:int = 1, retry_delay:float = 1.0):
        def decorator(func:Callable):
            jobs = [serialize(data, retrys, retry_delay) for data in func()]
            self.client.push_topic(topic, jobs)
        return decorator        

    def topics(self, retrys:int = 1, retry_delay:float = 1.0):
        def decorator(func:Callable):
            jobs = {topic:serialize(data, retrys, retry_delay) for topic, data in func().items()}
            self.client.push_topics(jobs)
        return decorator        

    def start_worker(self, client_str:str, conn_url:str, assignor:Assignor = Assignor.PriorityOne, batch:int = 0, retrys:int = 10, retry_delay:int = 1):
        process = Process(
            target=start_worker, 
            args=(client_str, conn_url, assignor, batch, retrys, retry_delay))
        process.daemon = True
        process.start()
        return process

    def start_workers(self, workers:int = 1, assignor:Assignor = Assignor.PriorityOne, batch:int = 1, retrys:int = 10, retry_delay:int = 1):
        client_str = func2str(self.client)
        conn_url = self.client.conn_params.geturl()

        while not self._stop:
            # 清理死进程
            for pid, process in self._workers.items():
                if not process.is_alive():
                    self._workers.pop(pid)
                    os.kill(pid, signal.SIGINT)

            # 补充新进程
            while len(self._workers)<workers:
                process = self.start_worker(client_str, conn_url, assignor, batch, retrys, retry_delay)
                self._workers[process.pid] = process
        
            # 启动新进程
            # for process in self._workers:
            #     process.start()

        for pid, process in self._workers.items():
            process.close()
            process.join()
