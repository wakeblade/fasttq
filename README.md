# fasttq

#### 介绍
FastTQ是一款由 [德波量化](http://www.dealbot.cn) 开源的基于消息队列的多进程分布式任务调度器，原应用于Dealbot量化策略高并发回测。

目前版本暂时仅支持Python语言，消息队列仅支持Redis。

#### 软件架构
FastTQ 非常轻量化，仅包含3个代码文件：
- client.py -- 主要封装用于访问保存任务主题、任务处理器和任务队列的消息队列的客户端 
- queue.py -- 主要封装任务处理器注册和任务注册的装饰器，以及启动任意多个Worker子进程
- worker.py -- 主要封装任务处理子进程


#### 安装教程

安装FastQ有两种方法：
1.  直接从 [github](https://github.com/wakeblade/fasttq) 或者 [gitee](https://gitee.com/wakeblade/fasttq) 下载源代码，然后置入源代码根目录使用
2.  从 [github](https://github.com/wakeblade/fasttq) 或者 [gitee](https://gitee.com/wakeblade/fasttq) 下载源代码后本地安装：python setup.py install
3.  使用pip安装：pip install fasttq

#### 使用说明

1. 例一： 单进程推送任务
```python
import requests

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
def push_urls(*args):
    return [
        "http://www.baidu.com",
        "http://www.bing.com"
    ]

if __name__ == "__main__":
    fq.start_workers(4, retry_delay=0.01)
```

2. 例二： 多进程推送任务
```python
from fasttq import FastQueue, RedisClient

client = RedisClient("redis://localhost:6379/0")
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
    fq.start_mp(7, retry_delay=0.01)
```

#### 参与贡献

如果您觉得 [FastTQ](https://gitee.com/wakeblade/fasttq) 对您工作或者学习有价值，欢迎提供赞助。您捐赠的金额将用于团队持续完善FastQ的新功能和性能。 
![赞赏码](https://gitee.com/wakeblade/x2trade/raw/master/zsm.jpg '赞赏码')
![赞赏码](https://github.com/wakeblade/fasttq/assets/47707905/deeb02cf-4d81-43c6-9d11-f2f04538de11 '赞赏码')