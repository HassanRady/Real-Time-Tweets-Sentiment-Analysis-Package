import json

import uvicorn
from fastapi import FastAPI

from TweetAnalysis.spark_streamer import SparkStreamer
from TweetAnalysis.predict import make_bulk_prediction
from TweetAnalysis.train_model import run_training



app = FastAPI()
ss = SparkStreamer()

@app.get('/')
async def index():
    return {"Hello": "From index"}

@app.post('/train')
async def train_model():
    run_training(save_result=True)
    return {"Model Training": "Done!"}


@app.get('/start_stream')
async def retrieve_tweets(topic):
    ss.start_stream(topic, False)
    return {"Stream": "started!!!"}

@app.get('/tweets')
async def retrieve_tweets(wait=0):
    df_tweets = ss.get_stream_data(int(wait), False)[['value', 'tweet']]
    return df_tweets.to_json()



@app.get('/predict')
async def predict(wait: int=0):

    x = ss.get_stream_data(wait, False)
    preds = make_bulk_prediction(x, True)

    return preds.to_json()



if __name__ == "__main__":
    uvicorn.run(app,host="127.0.0.1",port=9000)


