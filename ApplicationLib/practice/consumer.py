import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


channel.queue_declare(queue='hello')


def callback(ch, method, properties, body):

    time.sleep(2)
    print(" [x] Received %r" % body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(callback,
                      queue='hello',
                      )


channel.start_consuming()
