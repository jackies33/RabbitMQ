




from fastapi import FastAPI, Request, HTTPException
import json
import pika
from datetime import datetime
import sys
import logging

from my_env import rbq_netbox_exchange,listen_host,server_port,rbq_producer_pass,\
    rbq_producer_login,rbq_alermanager_exchange,rbq_lb_host


message_logger = logging.getLogger('recieved_messages')
message_logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('/var/log/rbq_custom/recieved_messages.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)
message_logger.addHandler(file_handler)

message_logger1 = logging.getLogger('recieved_messages_for_alarms')
message_logger1.setLevel(logging.INFO)
file_handler1 = logging.FileHandler('/var/log/rbq_custom/recieved_messages_for_alarms.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler1.setFormatter(formatter)
message_logger1.addHandler(file_handler1)



app = FastAPI()


def send_to_rabbitmq(route_key: str, message: str):
    credentials = pika.PlainCredentials(rbq_producer_login, rbq_producer_pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rbq_lb_host,
        credentials=credentials
    ))
    channel = connection.channel()
    if "nb" in str(route_key):
        channel.basic_publish(
            exchange=rbq_netbox_exchange,
            routing_key=route_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    elif "alarms" in str(route_key):
        channel.basic_publish(
            exchange=rbq_alermanager_exchange,
            routing_key=route_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    connection.close()




@app.post('/rabbitmq_proxy/{route_key}')
async def webhook_handler(route_key: str, request: Request):
    current_time = str(datetime.now()).split('.')[0]
    try:
        # Read raw data from the request
        raw_data = await request.body()
        raw_data_str = raw_data.decode('utf-8')
        #message_logger.info(f"Received raw data from: {route_key}: {raw_data}")

        # Try to decode the data as JSON
        try:
            data = json.loads(raw_data)
            message_logger.info(f"Received JSON message from: {route_key}: {data}")
            if "alarm" in route_key:
                message_logger1.info(f"Received JSON message from: {route_key}: {data}")
            message = json.dumps(data)
        except json.JSONDecodeError:
            # If JSON decoding fails, use the raw data
            message_logger.info(f"Received non-JSON message from: {route_key}: {raw_data}")
            if "alarm" in route_key:
                message_logger1.info(f"Received non-JSON message from: {route_key}: {raw_data_str}")
            data = json.loads(raw_data_str)
            message = json.dumps(data)
            #message = raw_data.decode('utf-8')

        print(f"Raw data received: {raw_data}")
        if not raw_data:
            print(f'\n\n{current_time}\n')
            raise HTTPException(status_code=400, detail="No data provided")

        print(f'\n\n{current_time}\n{message}\n\n')

        # Send message to RabbitMQ
        send_to_rabbitmq(route_key, message)
        return {"message": f"Message sent to RabbitMQ queue: {route_key}"}

    except HTTPException as e:
        print(f'\n\n{current_time}\n')
        print(f"HTTP error: {e.detail}\n")
    except Exception as e:
        print(f'\n\n{current_time}')
        print(f"An error occurred: {e}")




if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host=listen_host, port=server_port)


