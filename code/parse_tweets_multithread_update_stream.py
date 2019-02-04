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
    def __init__(self, database, table_target, table_mention):
        self.logger = logging.getLogger(__name__)
        self.database = database
        self.table_target = table_target
        self.table_mention = table_mention
        # self.table_quoted = table_quoted
        # self.table_retweet = table_retweet
        self.common_query = (" (ScreenNameOrg, TweetIDOrg, CreatedTime, Entities, FavoriteCount, TweetID, Reply, " 
                            "Quoted, Retweet, ReplyScreenName, ReplyUserID, ReplyTweetID, QuotedUserID, " 
                            "QuotedScreenName, QuotedTweetID, RetweetUserID, RetweetScreenName, RetweetTweetID, " 
                             "RetweetCount, Text, UserID, ScreenName) VALUES (%(ScreenNameOrg)s, %(TweetIDOrg)s, "
                             "%(CreatedTime)s, %(Entities)s, %(FavoriteCount)s, %(TweetID)s, %(Reply)s, %(Quoted)s, "
                             "%(Retweet)s, %(ReplyScreenName)s, %(ReplyUserID)s, %(ReplyTweetID)s, %(QuotedUserID)s, "
                             "%(QuotedScreenName)s, %(QuotedTweetID)s, %(RetweetUserID)s, %(RetweetScreenName)s, "
                             "%(RetweetTweetID)s, %(RetweetCount)s, %(Text)s, %(UserID)s, %(ScreenName)s) ")
        self.common_param = ("(`ScreenNameOrg` VARCHAR(63),"
                     "`TweetIDOrg` VARCHAR(63),"
                     "`CreatedTime` DATETIME(6)," 
                     "`Entities` TINYINT(1),"
                     "`FavoriteCount` INT(31),"
                     "`TweetID` VARCHAR(63),"
                     "`Reply` TINYINT(1),"
                     "`Quoted` TINYINT(1),"
                     "`Retweet` TINYINT(1),"
                     "`ReplyScreenName` VARCHAR(63),"
                     "`ReplyUserID` VARCHAR(63),"
                     "`ReplyTweetID` VARCHAR(63),"
                     "`QuotedUserID` VARCHAR(63),"
                     "`QuotedScreenName` VARCHAR(63),"
                     "`QuotedTweetID` VARCHAR(63),"
                     "`RetweetUserID` VARCHAR(63),"
                     "`RetweetScreenName` VARCHAR(63),"
                     "`RetweetTweetID` VARCHAR(63),"
                     "`RetweetCount` INT(31),"
                     "`Text` VARCHAR(255),"
                     "`UserID` VARCHAR(63),"
                     "`ScreenName` VARCHAR(63),"
                     "primary key (`TweetIDOrg`, `TweetID`)"
                    ") ENGINE=InnoDB ")

        self.param = self.common_param
        self.insert_query = ("INSERT INTO " + self.table_target + self.common_query)
        self.cnx = ct.create_table(self.database, self.table_target, self.param)
        # self.cursor = self.cnx.cursor(buffered=True)
        self.cursor = self.cnx.cursor()

        self.param_mention = (
            " ( `mention_ScreenName` VARCHAR(63),"
            " `mention_UserID` VARCHAR(63),"
            " `TweetID` VARCHAR(63),"
            " `ScreenName` VARCHAR(63),"
            " `TweetIDOrg` VARCHAR(63),"
            "primary key (`TweetIDOrg`, `TweetID`, `mention_UserID`)"
            ") ENGINE=InnoDB")
        self.insert_query_mention = ("INSERT INTO " + self.table_mention +
                                     " (mention_ScreenName, mention_UserID,  TweetId, ScreenName, TweetIDOrg) VALUES "
                                     "(%(mention_ScreenName)s, %(mention_UserID)s, %(TweetID)s, "
                                     "%(ScreenName)s, %(TweetIDOrg)s)")
        self.cnx_mention = ct.create_table(self.database, self.table_mention, self.param_mention)
        # self.cursor_mention = self.cnx_mention.cursor(buffered=True)
        self.cursor_mention = self.cnx_mention.cursor()

        # self.param_quoted = self.common_param
        # self.insert_query_quoted = ("INSERT INTO " + self.table_quoted + self.common_query)
        # ct.create_table(self.database, self.table_quoted, self.param_quoted)
        # self.cursor_quoted = self.cnx_quoted.cursor(buffered=True)
        #
        # self.param_retweet = self.common_param
        # self.insert_query_retweet = ("INSERT INTO " + self.table_retweet + self.common_query)
        # ct.create_table(self.database, self.table_retweet, self.param_retweet)
        # self.cursor_retweet = self.cnx_retweet.cursor(buffered=True)

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

    def process_retweet(self, rtweet, ScreenName, TweetID, TweetIDRoot=None):
        # self.table_retweet_quoted = self.table_quoted + '_retweet'
        # self.table_retweet_retweet = self.table_retweet + '_retweet'
        # self.table_retweet_mention = self.table_mention + '_retweet'
        if rtweet is None:
            return None, None, None
        else:
            self.Parser_tweet_rtweet = parse_tweets(self.database, self.table_target, self.table_mention)
            # Parser_tweet_rtweet.insert_query = ("INSERT INTO " + self.table_retweet + self.common_query)
            tweet_parser(rtweet, self.Parser_tweet_rtweet, ScreenName, TweetID, TweetIDRoot)
            # self.Parser_tweet_rtweet.cursor.close()
            # self.Parser_tweet_rtweet.cursor_mention.close()
            # self.Parser_tweet_rtweet.cnx.close()
            # self.Parser_tweet_rtweet.cnx_mention.close()
            return rtweet['user']['id_str'], rtweet['user']['screen_name'], rtweet['id_str']

    def connect_tables(self, database, table_target, table_mention):
        # connect to database to update tables
        self.database = database
        self.table_target = table_target
        self.table_mention = table_mention
        # self.table_quoted = table_quoted
        # self.table_retweet = table_retweet



def tweet_parser(myStr, Parser_tweet, ScreenNameOrg=None, TweetIDOrg=None, TweetIDRoot = None):
    logger = logging.getLogger(__name__)
    CreatedTime = Parser_tweet.process_create_time(myStr['created_at'])
    TweetID = myStr['id_str']
    ScreenName = myStr['user']['screen_name']
    if ScreenNameOrg is None:
        ScreenNameOrg = ScreenName
    if TweetIDOrg is None:
        TweetIDOrg = TweetIDRoot
    Entities = Parser_tweet.process_entities(myStr['entities'], ScreenName, TweetID, TweetIDRoot)
    FavoriteCount = myStr['favorite_count']
    Reply = True if myStr['in_reply_to_status_id'] is not None else False
    Quoted = myStr['is_quote_status']
    Retweet = myStr['retweeted']
    ReplyScreenName = myStr['in_reply_to_screen_name']
    ReplyUserID = myStr['in_reply_to_user_id_str']
    ReplyTweetID = myStr['in_reply_to_status_id_str']
    QuotedUserID = QuotedScreenName = QuotedTweetID = None
    RetweetUserID = RetweetScreenName = RetweetTweetID = None
    if 'quoted_status' in myStr:
        QuotedUserID, QuotedScreenName = Parser_tweet.process_quoted_tweet(myStr['quoted_status'],
                                          ScreenName, TweetID, TweetIDRoot)
        logger.debug("tweet id {} is a quote tweet".format(TweetID))
    if 'retweeted_status' in myStr:
        RetweetUserID, RetweetScreenName, RetweetTweetID = Parser_tweet.process_retweet(
            myStr['retweeted_status'], ScreenName, TweetID, TweetIDRoot)
        Quoted = False
        Retweet = True
        logger.debug("tweet id {} is a retweet".format(TweetID))

    if 'quoted_status_id_str' in myStr:
        QuotedTweetID = myStr['quoted_status_id_str']

    RetweetCount = myStr['retweet_count']

    Text = Parser_tweet.preprocessWord(myStr['text'])
    UserID = myStr['user']['id_str']
    ScreenName = myStr['user']['screen_name']

    insert_data = {
        "ScreenNameOrg": ScreenNameOrg,
        "TweetIDOrg": TweetIDOrg,
        "CreatedTime": CreatedTime,
        "Entities": Entities,
        "FavoriteCount": FavoriteCount,
        "TweetID": TweetID,
        "Reply": Reply,
        "Quoted": Quoted,
        "Retweet": Retweet,
        "ReplyScreenName": ReplyScreenName,
        "ReplyUserID": ReplyUserID,
        "ReplyTweetID": ReplyTweetID,
        "QuotedUserID": QuotedUserID,
        "QuotedScreenName": QuotedScreenName,
        "QuotedTweetID": QuotedTweetID,
        "RetweetUserID": RetweetUserID,
        "RetweetTweetID": RetweetTweetID,
        "RetweetScreenName": RetweetScreenName,
        "RetweetCount": RetweetCount,
        "Text": Text,
        "UserID": UserID,
        "ScreenName": ScreenName, }
    # print insert_data, insert_query
    try:
        Parser_tweet.cursor.execute(Parser_tweet.insert_query, insert_data)
        Parser_tweet.cnx.commit()
    except mysql.connector.Error as e:
        logger.debug(e)
        pass


class process_tweets_data(object):
    # def __init__(self, files, fileDir, filesrc, Parser_tweet, ExistUser):
    def __init__(self, files, fileDir, filesrc, Parser_tweet):
        self.logger = logging.getLogger(__name__)
        self.files = files
        self.fileDir = fileDir
        self.filesrc = filesrc
        self.Parser_tweet = Parser_tweet
        # self.ExistUser = ExistUser

    def process_tweets_data(self, lock):
        # #
        self.count = 0
        self.lock = lock
        for self.filename in self.files:
            self.logger.info("Process file {}".format(self.filename))
            if self.count > 0:
                shutil.move(self.user_file, self.dst)
                self.logger.info("moving file to destination {}".format(self.dst))
            self.user_file = self.filesrc +'/'+ self.filename
            with open(self.user_file, encoding='utf-8') as self.onefile:
                self.dst = self.fileDir + '/processed/' + self.filename
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
                    self.myStr = json.loads(self.line)  ## if the json file is stored using double quote, use this line
                    # self.myStr = ast.literal_eval(str(self.line))  ## if the json file is with single ', use this line
                    self.idtest = self.myStr['id_str']
                    # if self.idtest in all_tweet:
                    #     self.logger.debug("File {} tweet ID {} has been processed".format(self.filename,
                    #                                                                       self.myStr['id_str']))
                    #     continue
                    # self.userid = self.myStr['user']['id_str']
                    # if self.userid in self.ExistUser:
                    self.lock.acquire()
                    TweetIDRoot = self.idtest
                    self.logger.debug("processing tweet {}".format(self.idtest))
                    tweet_parser(self.myStr, self.Parser_tweet, TweetIDRoot=TweetIDRoot)
                    # all_tweet.append(self.idtest)
                    self.lock.release()

            self.count += 1
        shutil.move(self.user_file, self.dst)
        self.logger.info("moving file to destination {}".format(self.dst))
        return

class mythread(threading.Thread):
    def __init__(self, threadnm, location, database, table_target, table_mention, lock):
                 # ExistUser, lock):
        threading.Thread.__init__(self)
        self.threadnm = threadnm
        self.location = location
        self.values = initvalue(self.threadnm, self.location)
        self.logger = get_logger(self.values.fileDir, self.threadnm)
        self.database = database
        self.table_target = table_target
        self.table_mention = table_mention
        # self.table_quoted = table_quoted
        # self.table_retweet = table_retweet
        # self.ExistUser = ExistUser
        self.lock = lock

    def run(self):

        if self.values.files.__len__() == 0:  # check if there are files in this folder
            self.logger.info("There are no files in this folder, program abort...")
            return
        self.logger.debug("Start parsing tweet thread {}".format(self.threadnm))
        self.logger.info("processing {} files".format(self.values.files.__len__()))
        self.logger.info("file source: {}".format(self.values.filesrc))
        self.Parser_tweet = parse_tweets(self.database, self.table_target, self.table_mention)
        tweet_process = process_tweets_data(self.values.files, self.values.fileDir,
                            # self.values.filesrc, self.Parser_tweet, self.ExistUser)
                                            self.values.filesrc, self.Parser_tweet)
        tweet_process.process_tweets_data(self.lock)
        self.Parser_tweet.cursor.close()
        self.Parser_tweet.cursor_mention.close()

        self.Parser_tweet.cnx.close()
        self.Parser_tweet.cnx_mention.close()



class initvalue(object):
    def __init__(self, batchnm, location):
        self.batchnm = batchnm
        self.location = location
        self.fileDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                                    # 'twitter_data/streaming_data/' + self.location))
                                                    'twitter_data/streaming_data/' + self.location))
        self.filesrc = self.fileDir + '/batch_' + self.batchnm
        self.files = os.listdir(self.filesrc)


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
    batchnm = [1, 2, 3]
    location = 'tas'
    database = 'twitter_tasmania_new'
    table_target = 'twitter_tweets_tasmania_stream_2018'
    table_mention = 'twitter_mention_tasmania_stream_2018'
    # table_quoted = 'twitter_quoted_tasmania_new'
    # table_retweet = 'twitter_retweet_tasmania_new'
    # table_users = 'twitter_users_tasmania'

    conn = ct.connect(database)
    cursor = conn.cursor()
    # query = "SELECT UserID FROM " + table_users
    # try:
    #     cursor.execute(query)
    #     ExistUser = cursor.fetchall()
    #     ExistUser = [x[0] for x in ExistUser]
    # except mysql.connector.Error as e:
    #     print(e)


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
    lock = threading.Lock()
    for i in batchnm:
        # thread = mythread(str(i), location, database, table_target, table_mention, table_quoted,
        #                   table_retweet, ExistUser, lock)
        thread = mythread(str(i), location, database, table_target, table_mention, lock)
        thread.setName("Tasmania_stream_data_batch_{}".format(i))
        thread.start()




if __name__ == '__main__':
    main()