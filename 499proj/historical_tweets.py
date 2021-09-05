#!/usr/bin/env python
# encoding: utf-8
import tweepy
import csv

from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError
import json

consumer_key = "PvW0mscwLhCpw6dq8v80B55dk"
consumer_secret = "iaLbXe3vGKJMIn02kwZWShz6Mch7I2P4Pbz0xNLUD0Q5eK0Kn0"
access_token = "1350904847772643329-Hx2ua3bRo1Ny5fkPW336fhT9J1tHvl"
access_token_secret = "vnCbLtAACzooz0PSyJVfQlsQFrfdo8TchdIWyMNXaRg4X"

def get_all_tweets_from_an_user(user_id):
    # Twitter only allows access to a users most recent 3240 tweets with this method

    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    alltweets_en = []
    # initialize a list to hold all the tweepy Tweets
    newt_weets = tweepy.Cursor(api.user_timeline, user_id=user_id, lang='en').items(200)
    for new_tweet in newt_weets:
        if new_tweet.lang == "en":
            alltweets_en.append(new_tweet)

    # tweet.place.full_name, tweet.place.country,
    # tweet.place.country_code
    outtweets = [[
        tweet.id, tweet.user.id_str, tweet.user.screen_name, tweet.created_at, tweet.source, tweet.in_reply_to_status_id_str,
        tweet.in_reply_to_screen_name, tweet.retweet_count, tweet.favorite_count, tweet.text
    ] for tweet in alltweets_en]

    print(f"    {len(alltweets_en)} tweets downloaded")

    # write the csv
    #with open(f'./doc/new_{user_id}_tweets.csv', 'a+') as f:
    with open(f'./doc/tweets.csv', 'a+', encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerows(outtweets)
    pass


if __name__ == '__main__':
    sample_id = []
    for line in open("streaming_tweets.json", "r"):
        try:
            tweet_dict = json.loads(line)
            tweet = Tweet(tweet_dict)
            print(f"tweet added, user id: {tweet.user_id}")
        except (json.JSONDecodeError, NotATweetError):
            pass

        if tweet.user_id not in sample_id:
            sample_id.append(tweet.user_id)
    print("Finish scanning file streaming_tweets.json.................................")

    with open(f'./doc/tweets.csv', 'a+') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "userid_str", "screen_name", "created_at", "source", "in_reply_to_status_id_str",
        "in_reply_to_screen_name", "retweet_count", "favorite_count", "text"])

    for u_id in sample_id:
        print(f"Starting fetch tweets from user id: {u_id}")
        get_all_tweets_from_an_user(u_id)
        print(f"fetching tweets from user id finished: {u_id}\n\n")



