#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/18 8:34 下午
# @Author  : Thomas
# @File    : new_task.py

"""

P --->  Q --->  C1
          --->  C2

应用场景：
耗时的任务拆分为一个个message交友Workers(C1,C2)异步完成

默认情况下，MQ以轮询的方式（round-robin）分发消息，每个消费者平均接收到的消息数是一样的。

"""

import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

# 队列持久化，为了？
channel.queue_declare('task_queue', durable=True)

message = " ".join(sys.argv[1:]) or 'Hello World'

# basic_publish的properties参数指定message的属性
channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2         # make message persistent
    )
)
print("[x] Sent %r" % message)
connection.close()
