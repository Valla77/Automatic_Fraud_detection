import requests
import time
import ccloud_lib
from datetime import datetime
import json
import pandas as pd
from confluent_kafka import Producer

# Rapid API setup -> https://rapidapi.com/amansharma2910/api/realstonks/
url = "https://real-time-payments-api.herokuapp.com/current-transactions"

# headers = {
# 	"X-RapidAPI-Key": "219b312925msh63f546c46e1c02fp18cd42jsn0ad4e6b86dae",
# 	"X-RapidAPI-Host": "realstonks.p.rapidapi.com"
# }

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

# Read arguments and configurations and initialize
args = ccloud_lib.parse_args()
config_file = args.config_file
topic = args.topic
conf = ccloud_lib.read_ccloud_config(config_file)

# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
producer = Producer(producer_conf)

# Create topic if needed
ccloud_lib.create_topic(conf, topic)

delivered_records = 0

try:
    while True:
        
        # response = requests.request("GET", url)
        response = requests.get(url)
        data = pd.read_json(response.json(),orient="split")

        record_key="Real-time transaction"
        record_value=pd.DataFrame({"cc_num": data["cc_num"],
                                "merchant": data["merchant"],
                                 "category": data["category"],
                                 "amt": data["amt"],
                                 "first": data["first"],
                                 "last": data["last"],
                                 "gender": data["gender"],
                                 "street": data["street"],
                                 "city": data["city"],
                                 "state": data["state"],
                                 "zip": data["zip"],
                                 "lat": data["lat"],
                                 "long": data["long"],
                                 "city_pop": data["city_pop"],
                                 "job": data["job"],
                                 "dob": data["dob"],
                                 "trans_num": data["trans_num"],
                                 "merch_lat": data["merch_lat"],
                                 "merch_long": data["merch_long"],
                                 "is_fraud": data["is_fraud"],
                                 "current_time": data["current_time"]
                                 }).to_json(orient="split")
        # record_value = pd.DataFrame(record_value)
        # record_value = (record_value).to_json()


        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        print(record_value)
        producer.poll(0)
        time.sleep(12)

except KeyboardInterrupt:
    producer.flush()