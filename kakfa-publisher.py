from confluent_kafka import  Producer
import json

with open('data/data.json') as datafile:
    mydata=json.load(datafile)

P=Producer({'bootstrap.servers': 'localhost'})
for data in mydata:
    airline = data['airline']
    data['airline_alias'] = airline['alias']
    data['airline_iata'] = airline['iata']
    data['airline_id'] = airline['id']
    data['airline_name'] = airline['name']
    data.pop('airline')
    data.pop('_id')
    print('Producing message: %s' % data)
    P.produce('topic_json', json.dumps(data))
P.flush()
