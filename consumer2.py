# -*- coding: utf-8 -*-
"""
Created on 2019-05-04

@author: Pablo Carrera Flórez de Quiñones & José Llanes Jurado

This script consume the curated tweets from Kafka and produce a dinamic
visualization as a final output. The visualization is a barplot of the most
active users reflecting the number of tweets publicated.
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
    tweets_grouped = tweets.groupby(by = "user").size()
    tweets_grouped = tweets_grouped.sort_values(ascending = False)
    tweets_grouped = tweets_grouped[:10][::-1]
    
    # Update the plot with the new data
    tweets_grouped.plot(x = "user", y = "count", kind = "barh", zorder = 2, width = 0.85, legend = None)
    plt.title("Most active users", weight = "bold")
    plt.xlabel("Number of tweets publicated")
    plt.ylabel("")
    plt.pause(0.001)