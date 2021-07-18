#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/18 8:04 上午
# @Author  : Thomas
# @File    : send.py

"""

  P ----> [m|m|m|m] Queue -->  C

Producer sends messages to the Queue.
The consumer receives message from that queue.


"""
import pika

# BlockingConnection:同步模式
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# create queue 'hello'
channel.queue_declare(queue='hello')

# message不能直接发送给queue, 需经exchange到达queue
# exchange=''：使用默认交换机，默认交换机允许我们将消息发送到指定队列中，队列名由routing_key指定
channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')
print(" [x] Sent 'Hello World!'")

# make sure the network buffers were flushed and our message was actually delivered to RabbitMQ
connection.close()
