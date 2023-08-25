# Example written based on the official 
# Confluent Kakfa Get started guide https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/consumer.py

from confluent_kafka import Consumer
import json
import ccloud_lib
import time
import pandas as pd
import numpy as np
from datetime import datetime
# import pickle as pic
import joblib
import io
import requests

# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

import prediction

# # lancer dans le terminal:
# echo "export SENDGRID_API_KEY='API_KEYS'" > sendgrid.env
# echo "sendgrid.env" >> .gitignore
# source ./sendgrid.env
# pip install sendgrid
# pip install scikit-learn==1.2.2
# python producer.py -f python.config -t topic1




# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "topic1" 

# Create Consumer instance
# 'auto.offset.reset=earliest' to start reading from the beginning of the
# topic if no committed offsets exist
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'my_transactions_consumer'
consumer_conf['auto.offset.reset'] = 'earliest' # This means that you will consume latest messages that your script haven't consumed yet!
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([TOPIC])

# Process messages
try:
    while True:
        msg = consumer.poll(1.0) # Search for all non-consumed events. It times out after 1 second
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            # Check for Kafka message
            record_key = msg.key()
            record_value = msg.value()
            data = json.loads(record_value)
            # pred = pd.read_json(data.json(),orient="split")
            # bytes_io = io.BytesIO(record_value)
            # pred = pd.read_json(bytes_io,orient="split")
            # weather = data["degrees_in_celsion"]
            data = pd.DataFrame(data['data'], columns=data['columns'])
            data_api = data[['cc_num', 'current_time']]
            data_info = data[['cc_num', 'current_time' ,'is_fraud']]
            pred = prediction.prediction(data)[0]
            # print(data.info())
            print(f"Current transaction: {data_info} has a prediction: {pred}") 
            # print(f"Transaction frauduleuse détectée : {data_api} avec une probabilité de fraude de {pred}")
            
            # print(info)
            pred = 1
            info = f"Transaction frauduleuse détectée : {data_api} avec une prediction de fraude de {pred}"

            if pred > 0.9:
                message = Mail(
                    from_email='kamila.catoire@gmail.com',
                    to_emails='kamila.catoire@gmail.com',
                    subject='Alerte de fraude',
                    html_content=info)
                try:
                    sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
                    response = sg.send(message)
                    print(response.status_code)
                    print(response.body)
                    print(response.headers)
                except Exception as e:
                    print(e.message)
                
            time.sleep(12) # Wait 12 seconds
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()
