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
import twitter_config

# Set API keys for Twitter
consumer_key = twitter_config.consumer_key
consumer_secret = twitter_config.consumer_secret
access_token = twitter_config.access_token
access_token_secret = twitter_config.access_token_secret

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
to_track = ["Valencia"]

l = StdOutListener()
stream = Stream(auth, l)
stream.filter(track = to_track, stall_warnings = True)