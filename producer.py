# -*- coding: utf-8 -*-
"""
Created on 2019-05-04

@author: Pablo Carrera Flórez de Quiñones & José Llanes Jurado

This script capture tweets form Twitter regarding the diferent key words 
introduced by the user and save them into Kafka, which
converts this into a external Kafka producer.
"""

# Import libraries
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from kafka import KafkaProducer
import json

# Set API keys for Twitter
consumer_key =  "s5V7ZLhIu8oO7nd9hPrUW7jai"
consumer_secret = "MZn4ewjiZMNOD335TtANG7Mgf16lGx6a3qejeyPZxjm1gn14jc"
access_token = "826382041105182721-iLCkaPKzSXCmW7GyIRgNFQWYOy9UXat"
access_token_secret = "Lg7v1jwARlgJy2hpuCvrqrYk9t4pt1hi8Hs4if88BdoR7"

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Create the Kafka producer
producer = KafkaProducer(bootstrap_servers = ["192.168.99.100:9092"], 
                         value_serializer = lambda x: json.dumps(x).encode("utf-8")
                         )

# Create the stream object
class StdOutListener(StreamListener):
    def on_data(self, data):
        print(data)
        producer.send("tweetsraw", data)
        return True
    def on_error(self, status):
        print (status)

# Make it work
to_track = ["España"]

l = StdOutListener()
stream = Stream(auth, l)
stream.filter(track = to_track)