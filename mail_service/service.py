import smtplib
import ssl
from email.message import EmailMessage
import csv
import time
import json
from confluent_kafka import Producer
import urllib.request
from confluent_kafka import Consumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import time

email_sender="raghuvararora@gmail.com"
password="vhhpcrklorwlgedj"
receiver="rarora2@binghamton.edu"


config_url="https://storage.googleapis.com/project_cloud420/client.properties"


topic_name = 'transactions_fraud'
def read_ccloud_config(config_file):
    conf = {}
    
    with urllib.request.urlopen(config_file) as fh:
        for line in fh:
            line = line.decode('utf-8').strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


def get_database():

    # Configure MongoDB connection
    u=urllib.parse.quote_plus("raghuvararora@gmail.com")

    p=urllib.parse.quote_plus("qwert_asdfg")
    uri = "mongodb+srv://%s:%s@cluster0.udtc1lx.mongodb.net/?retryWrites=true&w=majority"%(u, p)
    uri = "mongodb+srv://u_ra:0LRHAlo8e01DMJGr@cluster0.udtc1lx.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri,server_api=ServerApi('1'))
    return client["User"]

# response = urllib.request.urlopen(config_url)


props=read_ccloud_config(config_url)
props["group.id"] = "python-group-2"
props["auto.offset.reset"] = "earliest"
consumer = Consumer(props)
consumer.subscribe([topic_name])
db_client=get_database()

userDetails = db_client["UserDetails"]

subject="ALERT: Potential Malicious Transaction Detected"
body='''fraudulent transaction detected on your account with us. Our monitoring systems have flagged a transaction that appears to be suspicious, and we want to make you aware of it as soon as possible.

Details of Transaction:

Transaction amount: $%s
Date and Time of transaction: %s
Location: %s
'''

try:
    while True:
        print("LISTENING FOR TRANSACTIONS")
        msg=consumer.poll(5)
        if msg is not None and msg.error() is None:
            input_str = msg.value().decode('utf-8')
            data=json.loads(input_str)
            user=userDetails.find_one({"cc_num":data["cc_num"]})
            print(user)
            if(user==None):
                continue
            em=EmailMessage()
            em["from"]=email_sender
            em["to"]=user["mail"]
            em["subject"]=subject
            em.set_content(body%(data["amt"], data["trans_date_trans_time"], data["city"]))
            context =ssl.create_default_context()
            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(email_sender, password)
                smtp.sendmail(email_sender, receiver, em.as_string())
except Exception as e:
    print(e)
    consumer.close()









