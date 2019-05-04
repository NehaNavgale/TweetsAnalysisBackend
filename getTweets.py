import json
import tweepy
from tweepy.streaming import StreamListener
from tweepy import Stream

with open("credentials.json", "r") as file:
    credentials = json.load(file)

py_tweets = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET']);
py_tweets.set_access_token(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET']);


class MyStreamListener(StreamListener):
    def on_data(self, data):
        with open('static/Tweets/NewAvengersTweets.json', 'a') as file:
            file.write(data)

        return True

    def on_error(self, status):
        print(status);


myStreamListener = MyStreamListener()
myStream = Stream(auth = py_tweets, listener=myStreamListener)

myStream.filter(track=['Avengers']);
