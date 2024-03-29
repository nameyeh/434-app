# [START gae_python37_app]
import flask
from flask import Flask, url_for
# , redirect, send_from_directory, request
from flask import jsonify
# import pandas as pd
from time import gmtime, strftime
# from textblob import TextBlob
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.cloud import bigquery
# import os
# import base64
# import sys
# from io import BytesIO
# from flask_api import status
from flasgger import Swagger
from sensible.loginit import logger 
log = logger(__name__)
# Swagger(app)
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
    
@app.route('/')
def home_page():
    BQclient = bigquery.Client()
    # language_client = language.LanguageServiceClient()
    sql  = '''
    SELECT *
    FROM
    `my-project-1520296049403.TwitterData.TweetsUnique`
    Order By created_at asc
    LIMIT 10
    '''
    df = BQclient.query(sql).to_dataframe()
    df=df.drop(columns=['user'])

    # df=df[['full_text','location']]
    df_table = df.to_html(classes=["table-bordered", "table-striped", "table-hover","table-condensed",
        "table-display: table","table-border-collapse: separate","thead-text-align:center"],header = 'true',index=False,table_id='1')

    return """
    <head>
    <meta charset="ISO-8859-1">
    <meta name="viewport" content="width=device-width, initial-scale=1"> 
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
    <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script> 
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script> 
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>
    <title>COVID-19 Tweets</title>
    </head>
    <body style="width: 880px; margin: auto;">
    <h1 style="color:red;font-weight: bold;">COVID-19 Tweets</h1>
    <h3>10 recent tweets mentioning COVID-19, filtered for verified accounts only</h3>
    <p style = "display: inline-block;"
    <div class="container">
        <div class="row">
            <div class="col-sm-6">
            <table>
            <table class="table">
                <thead-light>
                <tbody>
                {}
                </tbody>
                </thead-light>
            </table>
            </div>
        </div>
    </div>
    </p>
    <!-- Latest compiled and minified JavaScript -->
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js" integrity="sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa" crossorigin="anonymous"></script>
    </body>
    """.format(df_table)


def hello():
    """Return a friendly HTTP greeting."""
    return 'COVID-19 twitter analysis'

@app.route('/html')
def html():
    return """
    <title>This is a Hello World World Page</title>
    <p><b>Hello World</b></p>
    <p>dx testing page </p>
    """
@app.route('/time')
def time():
    my_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    print("This was the time I returned")
    my_time_dict = {"time": my_time}
    return jsonify(my_time_dict)

@app.route('/nlp-entities/')
def sample_analyze_entities():
    BQclient = bigquery.Client()
    sql  = "SELECT * FROM  `my-project-1520296049403.TwitterData.TweetsUnique` Order By created_at asc LIMIT 10"

    df = BQclient.query(sql).to_dataframe()
    text_content = df['full_text'][0]

    client = language.LanguageServiceClient()

    type_ = enums.Document.Type.PLAIN_TEXT
        # language = "en"
    document = {"content": text_content, "type": type_}
    encoding_type = enums.EncodingType.UTF8
    response = client.analyze_entities(document, encoding_type=encoding_type)
    
    entities_names = [entity.name for entity in  response.entities]
    entities_types = [(enums.Entity.Type(entity.type).name) for entity in  response.entities]
    entities_salience = [entity.salience for entity in  response.entities]
    
    zipped_name_types_salience = list(zip(entities_names,entities_types,entities_salience))

    val = {
           "INPUT TEXT": text_content,
           '# of entities found': (len(response.entities)), 
           'entity Name, Type, & salience/importance ': zipped_name_types_salience}
    return jsonify(val)

@app.route('/nlp-sentiment/')
def get_sentiment_scores():
    BQclient = bigquery.Client()
    language_client = language.LanguageServiceClient()
    sql  = '''
    SELECT *
    FROM
    `my-project-1520296049403.TwitterData.TweetsUnique`
    Order By created_at asc
    LIMIT 10
    '''
    df = BQclient.query(sql).to_dataframe()
    # text_content = df['full_text'][1]
        # content = text_content
    scores = []
    mags = []
    tweets = []
    for x in df['full_text']: 
        content = x 
        document = types.Document(content=content, type=enums.Document.Type.PLAIN_TEXT)
        sentiments = language_client.analyze_sentiment(document=document)
        score = sentiments.document_sentiment.score
        magnitude = sentiments.document_sentiment.magnitude
        tweets.append(content)
        scores.append(score)
        mags.append(magnitude)

    d = [ { 'INPUT_TEXT': x, 'sentiment_score': y, 'sentiment_magnitude': z } 
        for x, y, z in zip(tweets, scores, mags) ]
    return jsonify(d)

if __name__ == '__main__':
    
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python37_app]
