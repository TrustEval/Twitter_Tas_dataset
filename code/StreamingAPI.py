from slistener import SListener
import tweepy
import mysql.connector
from mysql.connector import errorcode
import auth_code as AC
import os
import time



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

def get_userID(database, table, batch_nm):
    batch_sz = 5000
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

    query = "SELECT UserID FROM " + table

    cursor.execute(query)

    results = cursor.fetchall()
    rangenm = (batch_nm-1)*batch_sz
    # print(rangenm, rangenm+batch_sz-1)
    ids = []
    for row in results[rangenm :(rangenm + batch_sz -1)]:
        if row[0] is not None:
            ids.append(row[0])
    # print ids

    return ids

def main():
    # streaming data from table
    appID = 1
    batch_nm = 3
    location = 'australia'
    database = 'twitter_tasmania'
    table = 'twitter_data_tasmania'
    # prefix = 'aus_loc_tas_users_batch'+'_'+str(batch_nm)
    prefix = 'commbank_mention_'
    datetime = time.strftime('%Y%m%d-%H%M%S')
    print("Using authcode from app {} and batch {}".format(appID, batch_nm))

    # OAuth process, using the keys and tokens from app lpy531 and batch 1
    authcode = AC.AuthCode(appID, batch_nm)
    auth = tweepy.OAuthHandler(authcode.consumer_key, authcode.consumer_secret)
    auth.set_access_token(authcode.access_key, authcode.access_secret)

    api = tweepy.API(auth)

    listen = SListener(api, prefix , location)
    stream = tweepy.Stream(auth, listen)
    # follow_set = get_userID(database, table, batch_nm)

    print("Streaming for tweets posted from Australia, and by users in tasmania, batch {}".format(batch_nm))

    try:
        # stream.filter(follow= follow_set, async= True, locations=[108.883637, -43.689318, 154.500308, -10.143083])
        # stream.filter(follow=follow_set, async=True)
        stream.filter(track=['cba'], async=True)
        #stream.sample()
    except:
        print("error!")
        stream.disconnect()

if __name__ == '__main__':
    main()