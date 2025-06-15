import pika

amqp_url = "amqps://hxssuxdc:YOUR_PASSWORD@puffin.rmq2.cloudamqp.com/hxssuxdc"
params = pika.URLParameters(amqp_url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

queue_name = "test_queue"
channel.queue_declare(queue=queue_name, durable=True)

def callback(ch, method, properties, body):
    print(f"Received: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print("Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
