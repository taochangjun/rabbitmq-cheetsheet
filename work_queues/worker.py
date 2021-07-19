#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/18 8:54 下午
# @Author  : Thomas
# @File    : worker.py

import pika
import time


# 定义一个回调函数来处理消息队列中的消息
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    print(" ack tag is:", method.delivery_tag)
    # 手动发送确认消息
    ch.basic_ack(method.delivery_tag)


connection = pika.BlockingConnection(
    pika.ConnectionParameters()
)
channel = connection.channel()
# 申明消息队列。当不确定生产者和消费者哪个先启动时，可以两边重复声明消息队列。
channel.queue_declare(queue='task_queue', durable=True)

# 公平调度
# 如果该消费者的channel上未确认的消息数达到了prefetch_count数，则不向该消费者发送消息
channel.basic_qos(prefetch_count=1)
tag = channel.basic_consume(consumer_callback=callback,
                            queue='task_queue',
                            no_ack=False,
                            consumer_tag='task_queue_worker')
print("consumer_tag is %s" % tag)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
