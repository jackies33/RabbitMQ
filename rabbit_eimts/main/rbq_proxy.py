


'''
for daemon setup script
create  - >> "mcedit /etc/systemd/system/rbq_proxy.service"
copy in rbq_proxy.service ->>
_______________________________

[Unit]
Description=Listen and classifier web hooks from main App through RabbitMQ

[Service]
ExecStart=/usr/bin/python3 /opt/rbq_custom/rbq_proxy.py
StandardOutput=file:/var/log/rbq_custom/output_sys.log
StandardError=file:/var/log/rbq_custom/error.log
Restart=always

[Install]
WantedBy=multi-user.target
_________________________________

<<----copy in rbq_proxy.service

run next commands -->>>
_____________________________
sudo systemctl daemon-reload
sudo systemctl enable rbq_proxy.service
sudo systemctl start rbq_proxy.service

______________________________

<<--- run next commands

'''





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

@app.post('/rabbitmq_proxy/{route_key}')# create path for main webhook
async def webhook_handler(route_key: str, request: Request):
    current_time = str(datetime.now()).split('.')[0]
    try:
        data = await request.json()
        message_logger.info(f"Recieved message from: {route_key}: {data}")
        if "alarm" in str(route_key):
            message_logger1.info(f"Recieved message from: {route_key}: {data}")
        print(f"Raw data received: {data}")
        if not data:
            print(f'\n\n{current_time}\n')
            raise HTTPException(status_code=400, detail="No data provided")
        print(f'\n\n{current_time}\n{data}\n\n')
        message = json.dumps(data)
        send_to_rabbitmq(route_key, message)
        return {"message": f"Message sent to RabbitMQ queue: {route_key}"}
    except HTTPException as e:
        print(f'\n\n{current_time}\n')
        print(f"HTTP error: {e.detail}\n")
    except json.JSONDecodeError as json_error:
        print(f'\n\n{current_time}\n')
        print(f"JSON decode error: {json_error}")
    except Exception as e:
        print(f'\n\n{current_time}')
        print(f"An error occurred: {e}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Connection error: {e}", file=sys.stderr)
        sys.exit(1)  # Exit the program with an error code
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)  # Exit the program with an error code



if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host=listen_host, port=server_port)








