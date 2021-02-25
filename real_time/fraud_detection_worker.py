import faust
from typing import List
import requests
import json

app = faust.App(
    'fraud_detection_app',
    broker='kafka://localhost:19092',
    value_serializer='raw',
)

kafka_topic = app.topic('hello')

@app.agent()
async def second_process(transactions):
    async for value in transactions:
        print('Same input data: ' + str(value))

@app.agent(kafka_topic, sink=[second_process])
async def process(transactions):
    async for value in transactions:
        #result = requests.post('http://127.0.0.1:5000/invocations', json=json.loads(value))
        print('Input data: ' + str(value))
        #print('Fraud detection result: ' + str(result.json()))
        yield json.dumps({"meow": json.loads(value)})

if __name__ == '__main__':    
    # run the consumer
    app.main()
