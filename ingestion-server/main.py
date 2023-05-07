import json
from confluent_kafka import Producer, Consumer
import urllib.request
import requests

def read_ccloud_config(config_url):
    conf = {}
    
    with urllib.request.urlopen(config_url) as fh:
        for line in fh:
            line = line.decode('utf-8').strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

def insertDocs(collection, database, dataSource,documents, headers ):
  payload = json.dumps({
    "collection": collection,
    "database": database,
    "dataSource": dataSource,
    "documents":documents
    
  })
  response = requests.request("POST", url, headers=headers, data=payload)
  return response



config_url="https://storage.googleapis.com/project_cloud420/client.properties"

url = "https://us-east-1.aws.data.mongodb-api.com/app/data-ockvs/endpoint/data/v1/action/insertMany"

headers = {
  'Content-Type': 'application/json',
  'Access-Control-Request-Headers': '*',
  'api-key': "6p6bwaFil5isFbQwZ2bvqIjWWiPPckfTqalTd118XRpf3lwpjsbOfEKkLmwKrWsv", 
}
def ingest(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    props = read_ccloud_config(config_url)
    props["group.id"] = "consumer1"
    props["auto.offset.reset"] = "earliest"
    consumer = Consumer(props)

    consumer.subscribe(["transactions_labeled"])
    batch=[]
    batchSize=0
    msg=consumer.poll(5)
    i=0
    count=0
    try:
        while msg is not None and msg.error() is None:
            input_str = msg.value().decode('utf-8')
            count+=1
            data=json.loads(input_str)
            batch.append(data)
            batchSize+=1
            if(batchSize==100):
                insertDocs("transactions", "transactions","Cluster0", batch, headers)
                batch=[]
                batchSize=0
                i+=1
            msg=consumer.poll(5)

        if(batchSize>0):
            insertDocs("transactions", "transactions","Cluster0", batch, headers)
        
        return "records ingested: "+str(count)
    except Exception as e:
        return e


