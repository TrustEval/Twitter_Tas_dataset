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


import simplejson as json
import tweepy
import csv, getpass, json, os, time, urllib
import sys
import auth_code
import mysql
from mysql.connector import errorcode

class MyModelParser(tweepy.parsers.ModelParser):
    def parse(self, method, payload):
        result = super(MyModelParser, self).parse(method, payload)
        result._payload = json.loads(payload)
        return result

def connect(database):
    """ Connect to MySQL database """
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database=database,
                                       user='root',
                                       password='')
        if conn.is_connected():
            print('Connected to MySQL database')
            return conn

    except Error as e:
        print(e)

def authTwitter(appID, batch_nm):
    """ Connect to Twitter API """
    authcode = auth_code.AuthCode(appID, batch_nm)
    auth = tweepy.OAuthHandler(authcode.consumer_key, authcode.consumer_secret)
    auth.set_access_token(authcode.access_key, authcode.access_secret)
    api = tweepy.API(auth, parser=MyModelParser(), wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    return api


# def read_total_list( in_filename ):
#
#     # read total fetch list csv
#     fp = open( in_filename, 'rb' )
#     reader = csv.reader( fp, delimiter=' ')
#
#     total_list = []
#     for row in reader:
#         total_list.append( row )
#
#     return total_list
#
#
# def purge_already_fetched( user, raw_dir ):
#
#     # list of tweet ids that still need downloading
#     rem_user = []
#
#     # check each tweet to see if we have it
#     for item in user:
#
#         # check if json file exists
#         tweet_file = raw_dir + item[0] + '.json'
#
#         if os.path.exists( tweet_file ):
#
#             # attempt to parse json file
#             # try:
#             #     parse_tweet_json( tweet_file )
#             #     print '--> already downloaded #' + item[0]
#             # except RuntimeError:
#             #     rem_user.append( item )
#             print "user %s downloaded already..." % item[0]
#             continue
#         else:
#             rem_user.append( item )
#
#     return rem_user

def get_user_tweets_id(api, user):

    alltweets = []

    # make initial request for most recent tweets (200 is the maximum allowed count)
    try:
        new_tweets = api.user_timeline(screen_name=user, count=200)
    except tweepy.TweepError as e:
        print 'User {} returns an error {}'.format(user, e.response.text)
        return False

    # save most recent tweets
    alltweets.extend(new_tweets)
    if len(alltweets) == 0:
        return False
    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:

        # all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name=user, count=200, max_id=oldest)

        # save most recent tweets
        alltweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        # print "...%s tweets downloaded so far" % (len(alltweets))
    return alltweets

def parse_tweet_json( filename ):
    
    # read tweet
    print 'opening: ' + filename
    fp = open( filename, 'rb' )

    # parse json
    try:
        tweet_json = json.load( fp )
    except ValueError:
        raise RuntimeError('error parsing json')

    # look for twitter api error msgs
    if 'error' in tweet_json:
        raise RuntimeError('error in downloaded tweet')

    # extract creation date and tweet text
    return [ tweet_json['created_at'], tweet_json['text'] ]


def download_tweets(api, fetch_list, raw_dir):
    # ensure raw data directory exists
    if not os.path.exists( raw_dir ):
        os.mkdir( raw_dir )

    # download tweets
    total = len(fetch_list)
    print total
    for idx, item in enumerate(fetch_list):
        file_name = raw_dir + '/' + item +'.json'
        if os.path.isfile(file_name):
            print "user {} has already been processed, continue to next one...".format(item)
            continue
        # print status
        print '--> downloading tweet #%s, %d of %d ' % (item,idx,total)
        alltweets = get_user_tweets_id(api, item)

        if alltweets is False:
            print "Something wrong with user {}".format(item)
            continue
        for tweet in alltweets:
            # print tweet.id_str
            try:
                status = api.get_status(tweet.id_str)
                with open(raw_dir+'/'+item+'.json', 'a') as f:
                    f.write(str(status._payload)+"\n")
                f.close
            except tweepy.TweepError, e:
                if tweepy.TweepError is "[{u'message': u'Sorry, that page does not exist', u'code': 34}]": pass

    return

def get_userID(database, table, batch_nm):
    batch_sz =15000
    try:
        cnx = connect(database)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    cursor = cnx.cursor(buffered=True)

    query = "SELECT ScreenName FROM " + table

    cursor.execute(query)

    results = cursor.fetchall()
    rangenm = (batch_nm-1)*batch_sz
    print rangenm, rangenm+batch_sz
    ids = []
    for row in results[rangenm :(rangenm + batch_sz)]:
        if row[0] is not None:
            ids.append(row[0])
    # print ids

    return ids

def main():
    location = 'tasmania'
    database = 'twitter_tasmania'
    table = 'twitter_users_tasmania'
    outputDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'twitter_data/rawData_'
                                             + location ))
    print outputDir
    
    #list of UserID or ScreenName in csv should be available in the same folder.
    #User id or Screen Name should be in the first column of the csv
    # file_name = (outputDir + "/twitter_data_darwin_batch" + batch_id + ".csv")
    # total_list = read_total_list(file_name)
    
    batch_id = 8
    appID = 1
    batch_nm = 1 # getting tweets of users 750-1000
    api = authTwitter(appID, batch_id)

    fetch_list = get_userID(database, table, batch_nm)
    # fetch_list = purge_already_fetched(total_list, outputDir)
    download_tweets(api, fetch_list, outputDir)

    return


if __name__ == '__main__':
    main()
