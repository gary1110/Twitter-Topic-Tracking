import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import csv
import json

consumer_key = "PvW0mscwLhCpw6dq8v80B55dk"
consumer_secret = "iaLbXe3vGKJMIn02kwZWShz6Mch7I2P4Pbz0xNLUD0Q5eK0Kn0"
access_token = "1350904847772643329-Hx2ua3bRo1Ny5fkPW336fhT9J1tHvl"
access_token_secret = "vnCbLtAACzooz0PSyJVfQlsQFrfdo8TchdIWyMNXaRg4X"


class StdOutListener(StreamListener):

    def on_data(self, data):
        # skip loading data that are not valid


        if 'delete' in data:
            print(data)
            print("delete found")
            return True
        else:
            print(data)

            with open('./streaming_tweets.json', 'a', encoding='utf8') as tf:
                tf.write(data)
                #json.dump(data,tf)
            return True



    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener()

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    #stream.filter(track=['python', 'javascript', 'ruby'])
    stream.sample()
