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
from google.cloud import bigquery

def remove_duplicates(request):
    projectid = 'my-project-1520296049403'
    client = bigquery.Client()
    sql  = '''
            SELECT *

            FROM
              `my-project-1520296049403.TwitterData.Tweets`
            Order By created_at asc
            '''
    df = client.query(sql).to_dataframe()
    print(df.shape)
    # drop duplicates 
    df = df.sort_values('id', ascending=False)
    df = df.drop_duplicates(subset='id', keep='first')
    print(df.shape)
    
 # append to bq 
    gbq.to_gbq(df, 'TwitterData.TweetsUnique', projectid, if_exists='append')

    return None
    

    
