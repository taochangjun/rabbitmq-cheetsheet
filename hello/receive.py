#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/18 8:56 上午
# @Author  : Thomas
# @File    : receive.py
"""
mq消费端手动ack,是保证可靠性消费的核心保障。

问题：
如果保证消费的消息不会丢失？
basic_consume时设置no_ack=false(1.12默认时false），消费成功后回调函数里手动ack.
因为no_ack=true的情况下，MQ任务message一旦被deliver出去了，就已被确认了，所以会立即将缓存中的message删除。
所以在Consumer异常时会导致消息丢失。

知识点：
自动ack与手动ack

注意：设置手动ack，如果没有执行ack，那么message会一直缓冲在队列中。

手动ack的时候MQ服务宕机了，重启这不是会造成重复消费吗，MQ重复消费的问题如何破？
"""

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

# 申明消息队列。当不确定生产者和消费者哪个先启动时，可以两边重复声明消息队列。
# queue_declare方法是幂等的，只会成功创建一次队列
channel.queue_declare(queue='hello')

# 从队列接收消息的原理：
# 定义一个回调函数callback， 当队列上接收到消息时，pika会自动调用这个callback函数 on_message_callback参数指定


# 定义一个回调函数来处理消息队列中的消息，这里是打印出来
def callback(channel, method, property, body):
    """
    :param channel: pika.Channel
    :param method:  pika.spec.Basic.Deliver
    :param property: pika.spec.BasicProperties
    :param body: str, unicode, or bytes (python 3.x)
    :return:
    """
    # 手动发送确认消息
    channel.basic_ack(delivery_tag=method.delivery_tag)
    # channel:
    # <BlockingChannel
    # impl=<Channel number=1
    # OPEN conn=<SelectConnection OPEN socket=('127.0.0.1', 60967)->('127.0.0.1', 5672)
    # params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>>>>
    print("channel is:", channel)
    # method:
    # <Basic.Deliver(['consumer_tag=ctag1.450274a6b633445d9df7c17bdcf761ab',
    # 'delivery_tag=1',
    # 'exchange=',
    # 'redelivered=False',
    # 'routing_key=hello'])>
    print("method is:", method)
    # <BasicProperties>
    print("property is:", property)
    # body-> byte类型, b'Hello World!'
    print("body is:", body)
    print("body decode is:", body.decode())


# Sends the AMQP 0-9-1 command Basic.Consume to the broker and binds messages
#         for the consumer_tag to the consumer callback. If you do not pass in
#         a consumer_tag, one will be automatically generated for you. Returns
#         the consumer tag.

# 注意：不同版本的pika的basic_consume参数可能不同，当前版本为'0.12.0'
# 设置回调函数，队列，customer_tag等
tag = channel.basic_consume(consumer_callback=callback,
                            queue='hello',
                            no_ack=False,
                            exclusive=False,
                            consumer_tag=None,
                            arguments=None)
# consumer_tag为None, 返回自动生成的tag, eg：ctag1.450274a6b633445d9df7c17bdcf761ab
print("tag is:", tag)
print(' [*] Waiting for messages. To exit press CTRL+C')
# 开始接收消息，并进入阻塞状态，队列里由信息才会调用callback进行处理
channel.start_consuming()
