import tweepy
import dml
import json
from datetime import datetime
from twitterscraper import query_tweets

# auth = tweepy.OAuthHandler(dml.auth['services']['Consumer']['API_key'],
#                            dml.auth['services']['Consumer']['API_secret_key'])
# auth.set_access_token(dml.auth['services']['Access']['Access_token'],
#                       dml.auth['services']['Access']['Access_token_secret'])
# api = tweepy.API(auth)
#
# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet)

# list_of_tweets = query_tweets("Trump OR Clinton", 10)
#Or save the retrieved tweets to file:
# file = open('output.txt', 'w')
# for tweet in query_tweets("Trump OR Clinton", 10):
#     file.write(tweet.to_string())
# file.close()


jsonfile = open("tweets.json","w")
list_of_tweets = query_tweets('Amman near:"Amman, Hashemite Kingdom of Jordan" within:15mi', 10,)
list_of_json = [] # create empty list to save multiple tweets which is type of json(dictionary)

for tweets in list_of_tweets: # for looping
    tweets.timestamp = datetime.strftime(tweets.timestamp, '%Y-%m-%d %H:%M:%S')
    list_of_json.append(vars(tweets))
json.dump(list_of_json, jsonfile)