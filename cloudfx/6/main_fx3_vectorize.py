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
import re,string 
from sklearn.feature_extraction.text import TfidfVectorizer
# from nltk.stem import WordNetLemmatizer 
import scipy

def vectorize_data(request):
    projectid = 'my-project-1520296049403'
    client = bigquery.Client()
    sql  = '''
            SELECT *

            FROM
              `my-project-1520296049403.TwitterData.TokenizedTweets`
            '''
    df = client.query(sql).to_dataframe()
    print(df.shape)
 
    final_processed_text_body= df['final_processed_text_body'].tolist()
    final_processed_text_ids= df['tweet_ids'].tolist()
    print(len(final_processed_text_body))
    print(len(final_processed_text_ids))
    
    NGRAM_RANGE = (1,3)
    # MAX_FEATURES = None
#     MIN_DF = 3
#     MAX_DF = 0.95
    SEED = 888
    
    Tfidf=TfidfVectorizer(ngram_range=NGRAM_RANGE,stop_words = 'english')

    # The vectorizer requires the 
    #stiched back together document.
    TFIDF_matrix=Tfidf.fit_transform(final_processed_text_body)     

    # append to bq 
    gbq.to_gbq(TFIDF_matrix, 'TwitterData.VectorizedTweets', projectid, if_exists='append')
    
    #creating datafram from TFIDF Matrix
    matrix=pd.DataFrame(TFIDF_matrix.toarray(), 
                        columns=Tfidf.get_feature_names(), 
                        index=final_processed_text_ids)

 # append to bq 
    gbq.to_gbq(matrix, 'TwitterData.TweetsMatrix', projectid, if_exists='append')

    return None
    

    
