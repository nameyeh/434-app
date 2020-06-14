



# Dependencies
import flask 
import numpy as np
from flask import Flask
from flask import jsonify
import tweepy as tw
import pandas as pd
from google.cloud import storage
import datetime 
import pandas_gbq as gbq
import os



def get_verif_tweets(request):
    # Twitter API Keys
    consumer_key = "PFwh2IvoxPELSRVFgAcOyGwwv"
    consumer_secret = "TWxCs75HgZ0KV8jX7LmIy7VTd0ttlnhN0Ex6O7nf7d95vZZZT9"
    projectid = 'my-project-1520296049403'

    # Setup Tweepy API Authentication
    auth = tw.AppAuthHandler(consumer_key, consumer_secret)
    api = tw.API(auth, wait_on_rate_limit=True,
                wait_on_rate_limit_notify=True)

# Define the search term and the date_since date as variables
    TODAY = datetime.datetime.now().strftime("%Y-%m-%d")
    search_words = "covid -filter:retweets filter:verified"
    date_since = TODAY
    N_ITEMS = 1000
    # Collect tweets
    collected_tweets = tw.Cursor(api.search,
                q=search_words,
                lang="en",
                since=date_since, tweet_mode='extended').items(N_ITEMS)


    # for tweet in collected_tweets: 
    #     print(tweet)
    list_of_tweets =[[tweet.full_text,\
                    tweet.user.screen_name,\
                    tweet.user.location,\
                    tweet.user.verified,\
                    tweet.created_at,\
                    tweet.id,\
                    tweet.user.statuses_count] for tweet in collected_tweets]

    df = pd.DataFrame(data=list_of_tweets, 
                        columns=['full_text',
                                'user', 'location',
                                'verified',
                                'created_at','id',
                                'statuses_count'])

    # generate datestamped filename
    currentDT = datetime.datetime.now().strftime("%m-%d-%y_%H%M")
    filename = "upload_tweets/tweets_"+currentDT+".csv"
    # upload to bucket 
    client = storage.Client()
    bucket = client.get_bucket('tweets-bucket1')
    bucket.blob(filename).upload_from_string(df.to_csv(index=False), 'text/csv')
    status200 = "successfully generated "+ filename
    print(status200)

    # append to bq 
    gbq.to_gbq(df, 'TwitterData.Tweets', projectid, if_exists='append')

    
    return None

Function to execute
get_verif_tweets