


import pika


import my_env


# Установка соединения с RabbitMQ сервером
connection = pika.BlockingConnection(pika.ConnectionParameters(my_env.rabbitmq_host))
channel = connection.channel()

# Объявление обменника (exchange)
channel.exchange_declare(exchange='main', exchange_type='direct')

# Создание очередей
channel.queue_declare(queue='nb_site_update1')
channel.queue_declare(queue='nb_site_update2')
channel.queue_declare(queue='nb_site_update3')
channel.queue_declare(queue='nb_site_update4')
channel.queue_declare(queue='nb_device_update1')


# Привязка очередей к обменнику с использованием ключей маршрутизации
channel.queue_bind(exchange='main', queue='nb_site_update1', routing_key='nb_site_update')
channel.queue_bind(exchange='main', queue='nb_site_update2', routing_key='nb_site_update')
channel.queue_bind(exchange='main', queue='nb_site_update3', routing_key='nb_site_update')
channel.queue_bind(exchange='main', queue='nb_site_update4', routing_key='nb_site_update_external')
channel.queue_bind(exchange='main', queue='nb_device_update1', routing_key='nb_device_update')


print("Queues and exchange are set up")

connection.close()