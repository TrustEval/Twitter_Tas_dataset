#
# Sanders-Twitter Sentiment Corpus Install Script
# Version 0.1
#
# Pulls tweet data from Twitter because ToS prevents distributing it directly.
#
# Right now we use unauthenticated requests, which are rate-limited to 150/hr.
# We use 125/hr to stay safe.  
#
# We could more than double the download speed by using authentication with
# OAuth logins.  But for now, this is too much of a PITA to implement.  Just let
# the script run over a weekend and you'll have all the data.
#
#   - Niek Sanders
#     njs@sananalytics.com
#     October 20, 2011
#
#
# Excuse the ugly code.  I threw this together as quickly as possible and I
# don't normally code in Python.
#
# consume Twitter data using REST API with delay to avoid limit
# input: list of Twitter ID in a csv file. Twitter ID should be in the first column of the CSV
# Please adjust the path of the CSV file in the main function
# slightly modified by Robertus for research purpose.
# last update by Peiyao LI


import simplejson as json
import tweepy
import csv, getpass, json, os, time, urllib
import sys

class MyModelParser(tweepy.parsers.ModelParser):
    def parse(self, method, payload):
        result = super(MyModelParser, self).parse(method, payload)
        result._payload = json.loads(payload)
        return result

def authTwitter():
    """ Connect to Twitter API using user credentials"""
    consumer_key = "y893zFKiGnQMZma2uFc0xXyvJ"
    consumer_secret = "kJO1LZr4BjChjJu8Hkdw3TcDpfqXRpDYSNLRUtQtuBgApR2DuH"
    access_key = "909650284699312128-BW9vKYC9ofvhN2CebyttDoQgt2HxgiW"
    access_secret = "ShUYrWbe1O25C8HeofFXPj0dG9k73Qh7THSigfWx4zOo0"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)

    return tweepy.API(auth, parser=MyModelParser(), wait_on_rate_limit=True, wait_on_rate_limit_notify=True)



def get_user_params():

    user_params = {}

    # get user input params
    user_params['inList']  = raw_input( '\nInput file [./corpus.csv]: ' )
    user_params['outList'] = raw_input( 'Results file [./full-corpus.csv]: ' )
    user_params['rawDir']  = raw_input( 'Raw data dir [./rawdata/]: ' )
    
    # apply defaults
    if user_params['inList']  == '': 
        user_params['inList'] = './corpus.csv'
    if user_params['outList'] == '': 
        user_params['outList'] = './full-corpus.csv'
    if user_params['rawDir']  == '': 
        user_params['rawDir'] = './rawdata/'

    return user_params


def dump_user_params( user_params ):

    # dump user params for confirmation
    print('Input:    '   + user_params['inList'])
    print('Output:   '   + user_params['outList'])
    print('Raw data: '   + user_params['rawDir'])
    return


def read_total_list( in_filename ):

    # read total fetch list csv
    fp = open(in_filename, "rt")
    reader = csv.reader(fp, delimiter=' ')

    total_list = []
    for row in reader:
        total_list.append( row )

    return total_list


def purge_already_fetched( user, raw_dir ):

    # list of tweet ids that still need downloading
    rem_user = []

    # check each tweet to see if we have it
    for item in user:

        # check if json file exists
        tweet_file = raw_dir + '/' + str(item[0]) + '.json'

        if os.path.exists( tweet_file ):

            # attempt to parse json file
            try:
                parse_tweet_json( tweet_file )
                print('--> already downloaded #' + item[0])
            except RuntimeError:
                rem_user.append( item )
        else:
            rem_user.append( item )

    return rem_user


def parse_tweet_json( filename ):
    
    # read tweet
    print('opening: ' + filename)

    # parse json
    try:
        with open(filename) as f_data:
            tweet_json = json.load(f_data)
            print(tweet_json)
    except ValueError as e:
        raise RuntimeError('error parsing json')

    # look for twitter api error msgs
    if 'error' in tweet_json:
        raise RuntimeError('error in downloaded tweet')

    # extract creation date and tweet text
    # return [ tweet_json['created_at'], tweet_json['text'] ]


def download_userinfo(api, fetch_list, raw_dir):
    

    # stay within rate limits
    # max_tweets_per_hr  = 125
    # download_pause_sec = 900 / max_tweets_per_hr

    # download tweets
    total = len(fetch_list)
    for idx, item in enumerate(fetch_list):
        user_info = []
        # print status
        print('--> downloading user #%s, %d of %d ' % (item[0], idx, total))
        try:
            user_info = api.get_user(screen_name=item[0])
        except tweepy.TweepError as e:
            print('User {} returns an error {}'.format(item[0], e.response.text))

        if user_info is None:
            print("Something wrong with user {}".format(item[0]))
            continue
        try:
            with open(raw_dir+'/' + item[0]+'.json', 'a', encoding='utf-8') as f:
                f.write(str(user_info._payload))

            f.close
        except:
            print("Error writing to json file...")

    return

def main():

    outputDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'twitter_data/cba/userInfo/'))
    
    #list of UserID or ScreenName in csv should be available in the same folder.
    #User id or Screen Name should be in the first column of the csv
    # ensure raw data directory exists
    if not os.path.exists(raw_dir):
        os.mkdir(raw_dir)
        
    total_list = read_total_list("./twitter_cba_screennames.csv")
    api = authTwitter()

    fetch_list = purge_already_fetched(total_list, outputDir)
    download_userinfo(api, fetch_list, outputDir)

    return


if __name__ == '__main__':
    main()
