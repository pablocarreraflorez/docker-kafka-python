# -*- coding: utf-8 -*-
"""
Created on 2019-05-04

@author: Pablo Carrera Flórez de Quiñones & José Llanes Jurado

This script consume the raw tweets from Kafka, curate them, and introduce 
them into another Kafka topic.
"""
# Import libraries
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime
from email.utils import parsedate_tz, mktime_tz

# Create the consumer and the producer
consumer = KafkaConsumer("tweetsraw",
                         bootstrap_servers = ["192.168.99.100:9092"],
                         value_deserializer = lambda x: json.loads(x.decode('utf-8')),
                         api_version = (1,3,4)
                         )

producer = KafkaProducer(bootstrap_servers = ["192.168.99.100:9092"], 
                         value_serializer = lambda x: json.dumps(x).encode("utf-8"),
                         api_version = (1,3,4)
                         )

# Define some specific functions
def to_local_time(tweet_time_string):
    timestamp = mktime_tz(parsedate_tz(tweet_time_string))
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

# Create the curated tweet
for tweet in consumer:
    tweet_raw = json.loads(tweet.value)   
    
    tweet_curated = {"topic" : tweet.topic,
                     "partition" : tweet.partition,
                     "offset" : tweet.offset,
                     "date" : to_local_time(tweet_raw["created_at"]),
                     "text" : tweet_raw["text"],
                     "user" : tweet_raw["user"]["screen_name"]
                     }
    
    #tweet_curated["date"] = re.sub("\d\d$", "00", tweet_curated["date"])
    
    print(tweet_curated)
    producer.send("tweetscurated", tweet_curated)
