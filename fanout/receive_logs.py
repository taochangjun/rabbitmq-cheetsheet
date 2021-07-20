#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/19 4:48 下午
# @Author  : Thomas
# @File    : receive_logs.py

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

# exclusive: Only allow access by the current connection
# 消费者断开后队列删除
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(queue=queue_name, exchange='logs')
print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] %r" % body)

channel.basic_consume(
    queue=queue_name,
    consumer_callback=callback,
    no_ack=False
)

channel.start_consuming()