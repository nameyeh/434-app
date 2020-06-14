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

def tokenize_data(request):
    projectid = 'my-project-1520296049403'
    client = bigquery.Client()
    sql  = '''
            SELECT *

            FROM
              `my-project-1520296049403.TwitterData.TweetsUnique`
            Order By created_at asc
            '''
    df = client.query(sql).to_dataframe()
    print(df.shape)
        
    print("reading in corpus and getting tweet_ids...")
    # read in corpus 
    data=df
    #create empty list to store tweet_ids
    tweet_ids=[]

    #for loop which appends the id to the id list
    for i in range(0,len(data)):
        temp_text=data['id'].iloc[i]
        tweet_ids.append(temp_text)
    tweet_ids[0]

    print("creating text_body...")
    #create empty list to store text documents
    text_body=[]

    #for loop which appends the text to the text_body list
    for i in range(0,len(data)):
        temp_text=data['full_text'].iloc[i] # updated!!
        text_body.append(temp_text)
    # text_body[0][0:500]

    #Note: the text_body is the unprocessed 
    #list of documents read directly form 
    #the df.
    print("text_body complete.")
    
    print("creating processed_text...")
    #empty list to store processed documents
    processed_text=[]
    #for loop to process the text to the processed_text list
    for doc in text_body:
        tokens=doc.split()
        re_punc = re.compile('[%s]' % re.escape(string.punctuation))
        # remove punctuation from each word
        tokens = [re_punc.sub('', w) for w in tokens]
        # remove remaining tokens that are not alphabetic
        tokens = [word for word in tokens if word.isalpha()]
        # filter out short tokens
        tokens = [word for word in tokens if len(word) > 5]
        #lowercase all words
        tokens = [word.lower() for word in tokens]
        # filter out stop words
        # s = WordNetLemmatizer()
        # tokens=[s.lemmatize(word) for word in tokens]
        text=tokens
        processed_text.append(text)

    a = list(map(lambda x: x if x else ["blank_List_element"], processed_text))
    processed_text=a
    # all(x for x in processed_text)
    print("processed_text complete.")
    
    print("creating final_processed_text...")
    #stitch back together individual words to reform body of text
    final_processed_text=[]

    for i in processed_text:
        temp_DSI=i[0]
        for k in range(1,len(i)):
            temp_DSI=temp_DSI+' '+i[k]
        final_processed_text.append(temp_DSI)
    final_processed_text[0]
    print("final_processed_text complete.")
    
    df = pd.DataFrame({"tweet_ids":tweet_ids,
                   "final_processed_text_body":final_processed_text})
    print(f"len(df)-->{len(df)}")
    df=df[df['final_processed_text_body']!='blank_List_element']
    print(f"len(df)-->{len(df)}")

    # final_processed_text_body= df['final_processed_text_body'].tolist()
    # final_processed_text_ids= df['tweet_ids'].tolist()
    # len(final_processed_text_body)
    # len(final_processed_text_ids)
    
    # append to bq 
    gbq.to_gbq(df, 'TwitterData.TokenizedTweets', projectid, if_exists='append')

    return None
    

    
