import os
import sys
import re
import nltk
import collections
import math
import pytz
from datetime import datetime
import ast
import json
import mysql.connector
import numpy as np
import CreateTable as ct
import shutil
import logging
import time
import threading


class parse_tweets(object):
    def __init__(self, database, table_target):
        self.logger = logging.getLogger(__name__)
        self.database = database
        self.table_target = table_target

        self.common_query = (" (UserID, TweetID, RelatedTweetID, Text, RelatedText) VALUES (%(UserID)s, %(TweetID)s, "
                             "%(RelatedTweetID)s, %(Text)s, %(RelatedText)s) ")
        self.common_param = ("(`UserID` VARCHAR(63),"
                     "`TweetID` VARCHAR(63),"
                     "`RelatedTweetID` VARCHAR(63),"
                     "`Text` VARCHAR(1000),"
                     "`RelatedText` VARCHAR(1000),"
                     "primary key (`UserID`, `TweetID`)"
                    ") ENGINE=InnoDB ")

        self.param = self.common_param
        self.insert_query = ("INSERT INTO " + self.table_target + self.common_query)
        self.cnx = ct.create_table(self.database, self.table_target, self.param)
        # self.cursor = self.cnx.cursor(buffered=True)
        self.cursor = self.cnx.cursor()


    def preprocessWord(self, text):
        self.text = re.sub('\'', ' ', text)
        self.text = re.sub(r'\\', ' ', self.text)

        return self.text

    def process_create_time(self, create_time):
        self.datetime_object = datetime.strptime(create_time,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)

        return self.datetime_object

    def process_entities(self, entities, ScreenName, TweetID, TweetIDRoot = None):
        self.entities = entities
        if self.entities['user_mentions'].__len__() > 0:
            for item in self.entities['user_mentions']:
                self.mention_ScreenName = item['screen_name']
                self.mention_UserID = item['id_str']

                self.insert_data_mention = {
                    "mention_ScreenName": self.mention_ScreenName,
                    "mention_UserID": self.mention_UserID,
                    "TweetID": TweetID,
                    "ScreenName": ScreenName,
                    "TweetIDOrg": TweetIDRoot,}
                try:
                    self.cursor_mention.execute(self.insert_query_mention, self.insert_data_mention)
                    self.cnx_mention.commit()
                except mysql.connector.Error as e:
                    self.logger.debug(e)
                    pass
            return True
        else:
            return False

    def process_quoted_tweet(self, qtweet, ScreenName, TweetID, TweetIDRoot = None):
        # self.table_quoted_quoted = self.table_quoted + '_quoted'
        # self.table_quoted_retweet = self.table_retweet + '_quoted'
        # self.table_quoted_mention = self.table_mention + '_quoted'
        if qtweet is None:
            return None, None
        else:
            self.Parser_tweet_qtweet = parse_tweets(self.database, self.table_target, self.table_mention)
            # Parser_tweet_qtweet.insert_query = ("INSERT INTO " + self.table_quoted + self.common_query)
            tweet_parser(qtweet, self.Parser_tweet_qtweet, ScreenName, TweetID, TweetIDRoot)
            # self.Parser_tweet_qtweet.cursor.close()
            # self.Parser_tweet_qtweet.cursor_mention.close()
            # self.Parser_tweet_qtweet.cnx.close()
            # self.Parser_tweet_qtweet.cnx_mention.close()
            return qtweet['user']['id_str'], qtweet['user']['screen_name']

    def process_retweet(self, rtweet, TweetID, TweetIDRoot=None):
        # self.table_retweet_quoted = self.table_quoted + '_retweet'
        # self.table_retweet_retweet = self.table_retweet + '_retweet'
        # self.table_retweet_mention = self.table_mention + '_retweet'
        if rtweet is None:
            return None, None, None
        else:
            rtxt = rtweet['text']
            return rtweet['id_str'], rtxt

    def connect_tables(self, database, table_target, table_mention):
        # connect to database to update tables
        self.database = database
        self.table_target = table_target
        self.table_mention = table_mention
        # self.table_quoted = table_quoted
        # self.table_retweet = table_retweet



def tweet_parser(myStr, Parser_tweet, ScreenNameOrg=None, TweetIDOrg=None, TweetIDRoot = None):
    logger = logging.getLogger(__name__)
    TweetID = myStr['id_str']
    UserID = myStr['user']['id_str']
    Retweet = myStr['retweeted']
    RetweetTweetID = RetweetText = None

    if 'retweeted_status' in myStr:
        RetweetTweetID, RetweetText = Parser_tweet.process_retweet(
            myStr['retweeted_status'], TweetID)
        Retweet = True
        logger.debug("tweet id {} is a retweet".format(TweetID))

    # if myStr['truncated']:
    #     Text = myStr['extended_tweet']['full_text']
    # else:
    Text = myStr['text']


    insert_data = {
        "UserID": UserID,
        "TweetID": TweetID,
        "RelatedTweetID": RetweetTweetID,
        "Text": Text,
        "RelatedText": RetweetText,}
    # print insert_data, insert_query
    try:
        Parser_tweet.cursor.execute(Parser_tweet.insert_query, insert_data)
        Parser_tweet.cnx.commit()
    except mysql.connector.Error as e:
        logger.debug(e)
        pass


class process_tweets_data(object):
    # def __init__(self, files, fileDir, filesrc, Parser_tweet, ExistUser):
    def __init__(self, files, filesrc, Parser_tweet):
        self.logger = logging.getLogger(__name__)
        self.files = files
        self.filesrc = filesrc
        self.Parser_tweet = Parser_tweet
        # self.ExistUser = ExistUser

    def process_tweets_data(self):
        # #
        self.filename = self.files
        self.logger.info("Process file {}".format(self.filename))
        self.user_file = self.filesrc +'/'+ self.filename
        with open(self.user_file, encoding='utf-8') as self.onefile:
            # self.dst = self.fileDir + '/processed/' + self.filename
            self.allline = self.onefile.readlines()
            self.n = -1
            while self.allline[self.n] == '\n':
                self.n -= 1
            # print self.allline[self.n]
            # self.myStr = ast.literal_eval(str(self.allline[self.n]))
            # self.myStr = json.loads(self.allline[self.n])
            # self.idtest = self.myStr['id_str']
            # print idtest
            # if self.idtest in all_tweet:
            #     self.logger.info("file {} has been processed".format(self.filename))
            #     self.count += 1
            #     continue
            for self.line in self.allline:
                if self.line == '\n':
                    continue
                # self.myStr = json.loads(self.line)  ## if the json file is stored using double quote, use this line
                self.myStr = ast.literal_eval(str(self.line))  ## if the json file is with single ', use this line
                tweet_parser(self.myStr, self.Parser_tweet)
                # all_tweet.append(self.idtest)

        return
# 
# class mythread(threading.Thread):
#     def __init__(self, location, database, table_target, lock):
#                  # ExistUser, lock):
#         threading.Thread.__init__(self)
#         self.threadnm = threadnm
#         self.location = location
#         self.values = initvalue(self.threadnm, self.location)
#         self.logger = get_logger(self.values.fileDir, self.threadnm)
#         self.database = database
#         self.table_target = table_target
#         # self.table_quoted = table_quoted
#         # self.table_retweet = table_retweet
#         # self.ExistUser = ExistUser
#         self.lock = lock
# 
#     def run(self):
# 
#         if self.values.files.__len__() == 0:  # check if there are files in this folder
#             self.logger.info("There are no files in this folder, program abort...")
#             return
#         self.logger.debug("Start parsing tweet thread {}".format(self.threadnm))
#         self.logger.info("processing {} files".format(self.values.files.__len__()))
#         self.logger.info("file source: {}".format(self.values.filesrc))
#         self.Parser_tweet = parse_tweets(self.database, self.table_target)
#         tweet_process = process_tweets_data(self.values.files, self.values.fileDir,
#                             # self.values.filesrc, self.Parser_tweet, self.ExistUser)
#                                             self.values.filesrc, self.Parser_tweet)
#         tweet_process.process_tweets_data(self.lock)
#         self.Parser_tweet.cursor.close()
#         self.Parser_tweet.cursor_mention.close()
# 
#         self.Parser_tweet.cnx.close()
#         self.Parser_tweet.cnx_mention.close()



# class initvalue(object):
#     def __init__(self,  location):
#         # self.batchnm = batchnm
#         self.location = location
#         self.fileDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
#                                                     # 'twitter_data/streaming_data/' + self.location))
#                                                     'twitter_data/streaming_data/' + self.location))
#         self.filesrc = self.fileDir + '/batch_' + self.batchnm
#         self.files = os.listdir(self.filesrc)


def get_logger(fileDir, batchnm):
    # create a file handler
    logger = logging.getLogger("threading_"+ batchnm)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=fileDir + '/log/parse_tweets_batch_' + batchnm + '.' + time.strftime(
                            '%Y%m%d-%H%M%S') + '.log',
                        filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    if batchnm == str(1):
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        # tell the handler to use this format
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger('').addHandler(console)

    return logger

def main():
    global all_tweet
    # batchnm = [1, 2, 3]
    location = 'cba'
    database = 'twitter_cba'
    table_target = 'twitter_tweets_cba'

    conn = ct.connect(database)
    cursor = conn.cursor()
    dir = os.path.split(__file__)
    sourcefolder = os.path.join(dir[0], 'CBA/')
    filename = 'CommBank.json'


    query = "SELECT TweetID FROM " + table_target
    try:
        cursor.execute(query)
        AllTweet = cursor.fetchall()
        AllTweet = [x[0] for x in AllTweet]
    except mysql.connector.Error as e:
        print(e)
        AllTweet = []
        pass

    all_tweet = AllTweet
    Parser_tweet = parse_tweets(database, table_target)
    tweet_process = process_tweets_data(filename, sourcefolder, Parser_tweet)
    # lock = threading.Lock()
    # for i in batchnm:
    #     # thread = mythread(str(i), location, database, table_target, table_mention, table_quoted,
    #     #                   table_retweet, ExistUser, lock)
    #     thread = mythread(str(i), location, database, table_target, lock)
    #     thread.setName("Tasmania_stream_data_batch_{}".format(i))
    #     thread.start()
    tweet_process.process_tweets_data()
    cursor.close()
    conn.close()



if __name__ == '__main__':
    main()