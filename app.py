from flask import Flask
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
spark = SparkSession.builder.appName("Tweets Analysis").getOrCreate()
import pandas as pd

# df = spark.read.json("static/Tweets/NewAvengersTweets.json")
# df.createOrReplaceTempView("tweets")
#
# query1 = spark.sql("select count(*) as count, user.location from tweets where user.location is not null group by "
#                        "user.location order by count desc Limit 20")
# query1.show()
# pd = query1.toPandas()
# pd.to_csv('static/Output/byCountry.csv', index=False)
#
# query2 = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'Avengers Infinity War' as Movie FROM tweets where text LIKE '%nfinity%' "
#                        "UNION SELECT COUNT(*) AS NumberOfTweets,'Avengers Age of Ultron' as Movie FROM tweets where text LIKE '%ltron%' "
#                        "UNION SELECT COUNT(*) AS NumberOfTweets, 'Avengers Civil War' as Movie FROM tweets where text LIKE '%ivil%' "
#                        "UNION SELECT COUNT(*) AS NumberOfTweets, 'Avengers End Game' as Movie FROM tweets where text LIKE '%end%'")
# query2.show()
# pd = query2.toPandas()
# pd.to_csv('static/Output/byMovie.csv', index=False)
#
# query3 = spark.sql("SELECT created_at AS Created FROM tweets")
# query3.select(to_date(query3.Created, 'yyyy-MM-dd').alias('Created')).collect()
# query3.show()


def load_countryData():
  csv_data = pd.read_csv("static/Output/byCountry.csv", sep=',')
  return csv_data

def load_movieData():
  csv_data = pd.read_csv("static/Output/byMovie.csv", sep=',')
  return csv_data

app = Flask(__name__)


@app.route('/api/byCountry')
def byCountry():
    data = load_countryData()
    return data.to_json(orient='records')


@app.route('/api/byMovie')
def byMovie():
    data = load_movieData()
    return data.to_json(orient='records')

@app.route('/api/byYear')
def byYear():
    query3 = spark.sql("SELECT created_at AS Created FROM tweets")
    query3.show()
    return 'Hello'
    # return pd.to_json(orient='records')

@app.route('/')
def hello():
    return 'HelloWorld'
    # return pd.to_json(orient='records')

if __name__ == '__main__':
    app.run()
