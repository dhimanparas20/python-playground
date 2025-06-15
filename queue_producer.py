import pika

amqp_url = "amqps://hxssuxdc:YOUR_PASSWORD@puffin.rmq2.cloudamqp.com/hxssuxdc"
params = pika.URLParameters(amqp_url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

queue_name = "test_queue"
channel.queue_declare(queue=queue_name, durable=True)

for i in range(10):
    message = f"Hello RabbitMQ! Message {i}"
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
    )
    print(f"Sent: {message}")

connection.close()
