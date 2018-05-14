from keys import *

import numpy as np
import pandas as pd

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy.utils import import_simplejson

import time

json = import_simplejson()

def Twitter_setup():
    '''
    Setup authentication and access using keys:
    '''
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    return auth

class CustomListener(StreamListener):
    def __init__(self, limit):
        self.count = 0
        self.limit = limit
        #tweet info
        self.tweet_created_at = np.array([])
        self.tweet_text = np.array([])
        self.tweet_source = np.array([])
        #user info
        self.user_created_at = np.array([])
        self.user_description = np.array([])
        self.user_location = np.array([]) 
        self.user_geo_enabled = np.array([])
        self.user_followers_count = np.array([])
        self.user_friends_count = np.array([])
        self.user_favourites_count = np.array([])
        self.user_statuses_count = np.array([])
        self.user_lang = np.array([])
        
    def on_data(self, raw_data):
        if (self.count < self.limit):
            print(self.count)
            data = json.loads(raw_data)
            if ('created_at' in data.keys()):
                if ((data['user'])['lang'] != 0):
                    #tweet
                    self.tweet_created_at = np.append(self.tweet_created_at,values=data['created_at'])
                    self.tweet_text = np.append(self.tweet_text,values=data['text'])
                    self.tweet_source = np.append(self.tweet_source,values=data['source'])
                    #user
                    self.user_created_at = np.append(self.user_created_at,values=(data['user'])['created_at'])
                    self.user_description = np.append(self.user_description,values=(data['user'])['description'])
                    self.user_location = np.append(self.user_location,values=(data['user'])['location'])
                    self.user_geo_enabled = np.append(self.user_geo_enabled,values=(data['user'])['geo_enabled'])
                    self.user_followers_count = np.append(self.user_followers_count,values=(data['user'])['followers_count'])
                    self.user_friends_count = np.append(self.user_friends_count,values=(data['user'])['friends_count'])
                    self.user_favourites_count = np.append(self.user_favourites_count,values=(data['user'])['favourites_count'])
                    self.user_statuses_count = np.append(self.user_statuses_count,values=(data['user'])['statuses_count'])
                    self.user_lang = np.append(self.user_lang,values=(data['user'])['lang'])

                    self.count += 1
            
            return True
        else:
            return False

    def get_tweet_data(self):
        return np.array([self.tweet_created_at, self.tweet_source, self.tweet_text])

    def get_user_data(self):
        return np.array([self.user_created_at, self.user_description, self.user_location,
                        self.user_geo_enabled, self.user_followers_count, self.user_friends_count, 
                        self.user_favourites_count, self.user_statuses_count, self.user_lang])

    def get_data(self):
        return np.concatenate((self.get_user_data(), self.get_tweet_data()), axis=0)


    def on_error(self, status):
        if status == 420: #returning False in on_error disconnects the stream
            time.sleep(60)
            return True

    def on_timeout(self):
        time.sleep(60)
        return True





def streaming_data():
    track_list = ['the', 'this', 'that', 'not','to', 'for', 'at', 'by', 
                            'you', 'a', 'an', 'of', 'with', 'I', 'be','am',
                            'is', 'are', 'what', 'I\'m', 'from', 'or', 'and', 'if', 
                            'as', 'we', 'it', 'there', 'so']
    limit = 50000  

    auth = Twitter_setup()
    listener = CustomListener(limit)

    try:
            twitterStream = Stream(auth, listener)
            twitterStream.filter(track=track_list, async=False)
    except:
            print('Error occurred when streaming.')
            time.sleep(5)
            
    data = listener.get_data()
    dataFrame = pd.DataFrame({'User_Created_At': data[0],
                                'User_Description': data[1], 
                                'User_Location': data[2],
                                'User_Geo_Enabled': data[3],
                                'User_Followers_Count': data[4],
                                'User_Friends_Count': data[5],
                                'User_Favourites_Count':data[6],
                                'User_Statuses_Count': data[7],
                                'User_Lang': data[8],

                                'Tweet_Time': data[9],
                                'Tweet_Source': data[10],
                                'Text': data[11]
                            })
    dataFrame.to_csv('Data.csv')

if __name__ == "__main__":
    streaming_data()

