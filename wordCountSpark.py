import time, re
from pyspark import SparkContext, SparkConf

import re
import json

tweets_text = []
readTweets = open("static/Tweets/NewAvengersTweets.json", "r")
for line in readTweets:
    try:
        tweet = json.loads(line)
        tweets_text.append(tweet["text"])
    except:
        continue

textString = ''.join(tweets_text);

# print(textString);

# print(str(tweets_text));

# hastags = [];
# re.findall(r"#(\w+)", tweets_text);

def getHashTags(text):
    tags = re.findall(r"#(\w+)", text);
    return tags

hashTags = getHashTags(textString)

def getURLS(text):
    urlText = re.findall(r'(https?://\S+)', text);
    return urlText

urlsText = getURLS(textString)

# print(hashTags)
# print(urlsText)
#
# def linesToWordsFunc(line):
#     wordsList = line.split()
#     print(wordsList)
#     wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
#     filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
#     return filtered

# def wordsToPairsFunc(word):
#     return (word, 1)
#
# def reduceToCount(a, b):
#     return (a + b)
def getCount(text, fileName):
    conf = SparkConf().setAppName("Words count").setMaster("local")
    sc = SparkContext(conf=conf)

    text_file = sc.parallelize(text)

    # text_file = sc.textFile("importTweets.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile("Output/"+fileName)
    sc.stop()

def main():
    getCount(hashTags, "hashtagCountSpark.txt")
    getCount(urlsText, "urlCountSpark.txt")
    # print(countHashtags)
    # countHashtags.saveAsTextFile("hashtagsCount.txt")
    # countURLS = getCount(hashTags)
    # countURLS.saveAsTextFile("urlsCount.txt")


if __name__ == "__main__":
    main()