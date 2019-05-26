# -*- coding: utf-8 -*-
"""
Created on 2019-05-04

@author: Pablo Carrera Flórez de Quiñones & José Llanes Jurado

This script consume the curated tweets from Kafka and produce a dinamic
visualization as a final output. The visualization is a plot of the number
of tweets publicated per minute.
"""

# Import libraries
from kafka import KafkaConsumer
from datetime import datetime
import json
import re
import pandas as pd
import matplotlib.pyplot as plt

# Create the consumer
consumer = KafkaConsumer("tweetscurated",
                         bootstrap_servers = ["192.168.99.100:9092"],
                         value_deserializer = lambda x: json.loads(x.decode('utf-8'))
                         )

# Consume the tweets into a dataframe
tweets = pd.DataFrame(columns = ["topic", "partition", "offset", "date", "text", "user"])

# Create the interactive figure
fig = plt.figure()
plt.ion()

for tweet in consumer:
    # Convert the tweet to a dataframe
    tweet_curated = tweet.value
    tweet_curated = {key : [tweet_curated[key]] for key in tweet_curated.keys()}
    tweet_curated = pd.DataFrame(tweet_curated)
    
    # Fix the date to the minute
    tweet_curated.loc[0, "date"] = re.sub("\d\d$", "00", tweet_curated.loc[0, "date"])
    tweet_curated.loc[0, "date"] = datetime.strptime(tweet_curated.loc[0, "date"], "%Y-%m-%d %H:%M:%S")
    
    # Create the dataframe for visualization
    tweets = tweets.append(tweet_curated)
    tweets_grouped = tweets.groupby(by = "date").size()
    
    # Update the plot with the new data
    fig.clear()
    plt.plot(tweets_grouped)
    plt.xticks(rotation = 45)
    plt.title("Tweets publicated per minute", weight = "bold")
    plt.pause(0.001)