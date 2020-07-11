import json
import pymongo
import pandas as pd
import sys
from confluent_kafka import Consumer, KafkaError
from pymongo  import MongoClient

MIN_COMMIT_COUNT = 1000

#extract the command line argument: host, port and database
USAGE="python sub-Process.py <MongoDB_Host> <MongoDB_Port> <MongoDB_Database>"

host=sys.argv[1]
port=int(sys.argv[2])
database=sys.argv[3]

'''
Set up MongoDB Client
'''
client=MongoClient(host, port)
db=client[database]
coll=db.agg_test

'''
Kafka Consumer settings
'''
c=Consumer({'bootstrap.servers':'localhost',
            'group.id':'mygroup',
            'default.topic.config':{'auto.offset.reset':'smallest'}})

c.subscriber(['topic_json'])

def aggregation_basic(msgs):
    df=pd.DataFrame(msgs)
    aggDF=df.groupby("airline_id").count()
    coll.insert_many(aggDF.to_dict('records'))

def consume():
    try:
        msg_count=0
        msgs=[]
        i=0
        while(i<10):
            msg=c.poll()
            if not msg.error():
                print('Received message: %s' % msg.value().decode('utf-8'))
                msgs.append(json.loads(msg.value()))
                msg_count += 1
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
            if i==9:# aggregate 10 messages at a time
                aggregation_basic(msgs)
                if msg_count % MIN_COMMIT_COUNT == 0:
                    c.commit()
                    i=0
                    msgs=[]
                else:
                    i+=1
    finally:
        c.close()

def main():
    if len(sys.argv) < 4:
        print("ERROR: Insufficient command line arguments supplied")
        print("       usage: '" + USAGE + "'")
        sys.exit(2)














