import urllib.request
import pandas as pd
import numpy as np
#import pickle
import json
import time
import sys
import datetime as dt
from pandas import json_normalize
from confluent_kafka import Consumer
from confluent_kafka import Producer
import pickle
import random
#print(pickle.__version__)
#getting the model store in google cloud
url = 'https://storage.googleapis.com/project_cloud420/finalModel.pkl'
#exception handling for the model url
config_file_url="https://storage.googleapis.com/project_cloud420/client.properties"
try:
    with urllib.request.urlopen(url) as f:
        data = f.read()
except urllib.error.URLError as e:
    print("Error opening URL:", e)
    sys.exit(1)


#loading the model
model2 = pickle.loads(data)
#reading the config file name client.properties
def read_ccloud_config(config_url):
    conf = {}
    
    with urllib.request.urlopen(config_url) as fh:
        for line in fh:
            line = line.decode('utf-8').strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf
# reading configuration defined for the kafka


props = read_ccloud_config(config_file_url)
props["group.id"] = "python-group-1"
props["auto.offset.reset"] = "earliest"
# producer_props = read_ccloud_config("client.properties")
producer = Producer(props)

consumer = Consumer(props)
consumer.subscribe(["transactions_raw"])
i=0
try:
    while True:
        msg = consumer.poll(5)
        if msg is not None and msg.error() is None:
            i+=1
            # print(msg.value().decode('utf-8'))
            input_str = msg.value().decode('utf-8')
            data=json.loads(input_str)
            del data['']
            producer.produce(topic="transactions_labeled", key=None, value=json.dumps(data))
            producer.flush()
            if(data["is_fraud"]=="1"):
                producer.produce(topic="transactions_fraud", key=None, value=json.dumps(data))
                producer.flush()           
            data=pd.json_normalize(data)
            data['dob']=pd.to_datetime(data['dob'])
            data['age']=dt.date.today().year-data['dob'].dt.year
            data=data[['category','amt','gender','age','is_fraud']]
            data=data.drop("is_fraud", axis='columns')
            # print(data.shape)
            data=pd.get_dummies(data, drop_first=True)
            row = np.array(['health_fitness', 100, 'F', 32])

            # One hot encode category
            categories = [ 'health_fitness', 'misc_pos', 'travel', 'kids_pets', 'shopping_pos', 'food_dining', 'home', 'entertainment', 'shopping_net', 'misc_net', 'grocery_pos', 'gas_transport', 'grocery_net']
            category_idx = categories.index(row[0])
            category_encoded = np.zeros(len(categories))
            category_encoded[category_idx] = 1

            gender_encoded = 1 if row[2] == 'F' else 0

            encoded_row = np.concatenate(([row[1]], [row[3]], category_encoded, [gender_encoded]))

            
            encoded_row=encoded_row.reshape(1,-1)
            # predicted=model2.predict(encoded_row)
            
            if(i%1000==0):
                print (data)
        # time.sleep(0.1)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
