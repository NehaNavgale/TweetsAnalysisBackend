from flask import Flask
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
spark = SparkSession.builder.appName("Tweets Analysis").getOrCreate()
import pandas as pd

df = spark.read.json("static/Tweets/NewAvengersTweets.json")
df.createOrReplaceTempView("tweets")

app = Flask(__name__)


@app.route('/api/byCountry')
def byCountry():
    query1 = spark.sql("select count(*) as count, user.location from tweets where user.location is not null group by "
                       "user.location order by count desc Limit 20")
    query1.show()
    pd = query1.toPandas()
    print(pd)
    return pd.to_json(orient='records')



if __name__ == '__main__':
    app.run()
