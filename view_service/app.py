from flask import Flask, jsonify
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import redis
import urllib.parse
import logging

logging.basicConfig(filename="logs.txt")
app = Flask(__name__)
TTL=60
def get_database():

    # Configure MongoDB connection
    u=urllib.parse.quote_plus("raghuvararora")

    p=urllib.parse.quote_plus("QKpTBzfqCPyYa6X2")
    uri = "mongodb+srv://%s:%s@cluster0.udtc1lx.mongodb.net/?retryWrites=true&w=majority"%(u, p)
    uri = "mongodb+srv://u_ra:0LRHAlo8e01DMJGr@cluster0.udtc1lx.mongodb.net/?retryWrites=true&w=majority"
    # print(uri)
    # app.config["MONGO_URI"] = uri
    # mongo = PyMongo(app)

    # Create a new client and connect to the server
    client = MongoClient(uri,server_api=ServerApi('1'))
    return client["transactions"]
# Send a ping to confirm a successful connection


def get_redis():

    r = redis.Redis(
    host='redis-16311.c57.us-east-1-4.ec2.cloud.redislabs.com',
    port=16311,
    password='biSw53IihqlNggEqqsUK6BMlFuMd9SGZ')
    r.set("test","value")
    return r

redis_client=get_redis()
# Route to retrieve users data
db=get_database()
@app.route("/transactions", methods=["GET"])
def get_transactions():
    try:
        # print(db.list_collections())
        collection_transactions=db["transactions"]
        # collection_transactions
        transactions=collection_transactions.find({"is_fraud":"1"},{'_id': 0})

        return jsonify(transactions=[user for user in transactions])
    except Exception as e:
        return "server error "+str(e), 500

@app.route("/transactions/metrics", methods=["GET"])
def get_transaction_metrics():
    # print(db.list_collections())
    try:
        reqstring="/transactions/metrics"
        if not redis_client.exists(reqstring):
            collection_transactions=db["transactions"]
            notFraud=collection_transactions.count_documents({"is_fraud":"0"})
            totalTransactions=collection_transactions.count_documents({})
            fraud=collection_transactions.count_documents({"is_fraud":"1"})

            # transactions=collection_transactions.find({},{'_id': 0})
            res=jsonify(notFraud=notFraud, totalTransactions=totalTransactions,fraudCount=fraud)
            redis_client.set(reqstring, res.get_data() )
            redis_client.expire(reqstring, TTL)
            return res.get_data()
        else:
            return redis_client.get(reqstring)
    except Exception as e:
        return "server error "+str(e), 500


@app.route("/transactions/category", methods=["GET"])
def get_transaction_category():
    pipeline = [
        {
            "$group": {
                "_id": "$category",
                "count": {
                    "$sum": "$is_fraud"
                }
            }
        }
    ]
    reqstring="/transactions/category"
    try:
        if not redis_client.exists(reqstring):
            collection_transactions=db["transactions"]
            agg_result=collection_transactions.aggregate(pipeline)

            # print(agg_result)
            res=jsonify(results=[result for result in agg_result])
            redis_client.set(reqstring, res.get_data() )
            redis_client.expire(reqstring, TTL)
            return res.get_data()
        else:
            return redis_client.get(reqstring)
    except Exception as e:
        return "server error "+str(e), 500


@app.route("/transactions/category/metrics", methods=["GET"])
def get_transaction_category_metrics():
    pipeline = [
        {
            "$group": {
            "_id": "$category",
            "total": {
                "$sum": 1
            },
            "fraudCount": {
                "$sum": {
                "$cond": [
                    {
                    "$eq": [
                        "$is_fraud",
                        "1"
                    ]
                    },
                    1,
                    0
                ]
                }
            }
            }
        }
    ]

    reqstring="/transactions/category/metrics"
    try:
        if not redis_client.exists(reqstring):
            collection_transactions=db["transactions"]
            agg_result=collection_transactions.aggregate(pipeline)

            # print(agg_result)

            res=jsonify(results=[result for result in agg_result])
            redis_client.set(reqstring, res.get_data() )
            redis_client.expire(reqstring, TTL)
            return res.get_data()
        else:
            return redis_client.get(reqstring)
    except Exception as e:
        return "server error "+str(e), 500


@app.route("/transactions/job/metrics", methods=["GET"])
def get_transaction_job_metrics():
    pipeline = [
        {
            "$group": {
            "_id": "$job",
            "totalcount": {
                "$sum": 1
            },
            "fraudCount": { "$sum": { "$cond": [{ "$eq": ["$is_fraud", "1"] }, 1, 0] } }
            }
        },
        {
            "$sort": {
            "fraudCount": -1
            }
        },

        {
            "$limit":10
        }
    ]

    reqstring="/transactions/job/metrics"
    try:
        if not redis_client.exists(reqstring):
            collection_transactions=db["transactions"]
            agg_result=collection_transactions.aggregate(pipeline)

            # print(agg_result)

            res=jsonify(results=[result for result in agg_result])
            redis_client.set(reqstring, res.get_data() )
            redis_client.expire(reqstring, TTL)
            return res.get_data()
        else:
            return redis_client.get(reqstring)
    except Exception as e:
        return "server error "+str(e), 500



@app.route("/transactions/time/metrics", methods=["GET"])
def get_transaction_time_metrics():
    pipeline = [
        {
            "$group": {
                "_id": {"$hour": {"$toDate": "$trans_date_trans_time"}},
                "total": {"$sum": 1},
                "frauds": {"$sum": {"$cond": [{"$eq": ["$is_fraud", "1"]}, 1, 0]}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "hour": "$_id",
                "fraud_percentage": {"$trunc": [{"$multiply": [{"$divide": ["$frauds", "$total"]}, 100]}, 2]}
            }
        },

    ]

    reqstring="/transactions/time/metrics"
    try:
        if not redis_client.exists(reqstring):
            collection_transactions=db["transactions"]
            agg_result=collection_transactions.aggregate(pipeline)
            res=jsonify(results=[result for result in agg_result])
            redis_client.set(reqstring, res.get_data() )
            redis_client.expire(reqstring, 10)
            return res.get_data()
        else:
            return redis_client.get(reqstring)
    except Exception as e:
        return "server error "+str(e), 500



@app.route("/transactions/gender/metrics", methods=["GET"])
def get_transaction_gender_metrics():
    pipeline = [
  {
    "$group": {
      "_id": "$gender",
      "count": {
        "$sum": 1
      },
      "total": {"$sum": 1},
      "is_fraud_count": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$is_fraud",
                "1"
              ]
            },
            1,
            0
          ]
        }
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "gender": "$_id",
    #   "percentage": {
    #     "$divide": [
    #       "$is_fraud_count",
    #       "$count"
    #     ]
    #   },
      "fraud_percentage": {"$trunc": [{"$multiply": [{"$divide": ["$is_fraud_count", "$total"]}, 100]}, 2]}
    }
  }
    ]

    reqstring="/transactions/gender/metrics"
    try:
        if not redis_client.exists(reqstring):
            collection_transactions=db["transactions"]
            agg_result=collection_transactions.aggregate(pipeline)
            res=jsonify(results=[result for result in agg_result])
            redis_client.set(reqstring, res.get_data() )
            redis_client.expire(reqstring, 10)
            return res.get_data()
        else:
            return redis_client.get(reqstring)
    except Exception as e:
        return "server error "+str(e), 500




if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
