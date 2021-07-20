#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/19 2:45 下午
# @Author  : Thomas
# @File    : emit_log.py
"""
一次将所有消息发送给多个消费者（广播）

exchange fanout类型：
P ----> Q1 ----> C1
  ----> Q2 ----> C2

"""

import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters()
)
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

message = ' '.join(sys.argv[1:]) or "info: Hello World"
channel.basic_publish(exchange='logs',
                      routing_key='',
                      body=message)
print("[x] Sent %r" % message)
connection.close()
