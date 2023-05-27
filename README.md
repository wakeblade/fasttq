# fastq

#### 介绍
FastQ是一款由 [德波量化](http://www.dealbot.cn) 开源的基于消息队列的多进程分布式任务调度器，原应用于Dealbot量化策略高并发回测。

目前版本暂时仅支持Python语言，消息队列仅支持Redis。

#### 软件架构
FastQ 非常轻量化，仅包含3个代码文件：
- client.py -- 主要封装用于访问保存任务主题、任务处理器和任务队列的消息队列的客户端 
- queue.py -- 主要封装任务处理器注册和任务注册的装饰器，以及启动任意多个Worker子进程
- worker.py -- 主要封装任务处理子进程


#### 安装教程

安装FastQ有两种方法：
1.  直接从 [github](https://github.com/wakeblade/fastq) 或者 [gitee](https://gitee.com/wakeblade/fastq) 下载源代码，然后置入源代码根目录使用
2.  使用pip安装：pip install fastq

#### 使用说明

```python
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
```

#### 参与贡献

如果您觉得 [FastQ](https://gitee.com/wakeblade/fastq) 对您工作或者学习有价值，欢迎提供赞助。您捐赠的金额将用于团队持续完善FastQ的新功能和性能。 
![赞赏码](https://gitee.com/wakeblade/x2trade/raw/master/zsm.jpg '赞赏码')