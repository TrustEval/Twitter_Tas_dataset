''' This is the main function of trust algortihm. The sturcture of the algorithm is as below:
- initialise each user and tweet value
- calculate each user value based on users component and tweets component
- calculate each tweet value based on user component and tweets component
- update user value and tweet value for next iteration
- start from step two with updated user and tweet value until converge
value set user = 0.4, tweet = 0.6'''
import os
import numpy as np
import ast
import json
import shutil
import mysql.connector
from mysql.connector import Error
import logging

import CreateTable as ct
import operator
import funclog
import time

import math

start_time = time.time()

def sigmoid(x, k, x0):
    return (1 / (1 + np.exp(-k*(x-x0))))


def mapping_func(trust_value, threshold, x1_ratio, x2_ratio, filelog):
    sorted_data = np.sort(trust_value)
    N = trust_value.__len__()
    insig = np.where(sorted_data <= threshold)
    threshid = insig[0][-1]
    tweetid = round(((N - threshid) * x1_ratio + threshid))
    x1 = sorted_data[int(tweetid)]
    tweetid_upper = int(round((N - threshid) * x2_ratio + threshid))
    x2 = sorted_data[tweetid_upper]
    A = math.log(1 / x2_ratio - 1) / math.log(1 / x1_ratio - 1)
    x0 = (x2 - A * x1) / (1 - A)
    k = math.log(1 / x1_ratio - 1) / (x0 - x1)
    filelog.logger.info('k = {}, x0 = {}, x1 = {}, x2 = {}'.format(k, x0, x1, x2))
    filelog.logger.info('id = {}, id_upper = {}'.format(tweetid, tweetid_upper))
    sig_data = [sigmoid(x, k, x0) for x in trust_value]
    # sig_data = trust_value
    # for i in range(sig_data.__len__()):
    #     if sig_data[i] > threshold:
    #         sig_data[i] = sigmoid(sig_data[i], k, x0)
    return sig_data


def spearman_rank(user_trust, iteration):
    N = user_trust[0].__len__()
    label = range(N)
    pre_user = np.ones((3, N))
    cur_user = np.ones((3, N))
    pre_user[0, :] = label
    cur_user[0, :] = label
    pre_user[1, :] = user_trust[iteration-1]
    cur_user[1, :] = user_trust[iteration]
    pre_user = np.transpose(pre_user)
    cur_user = np.transpose(cur_user)
    pre_user = pre_user[pre_user[:, 1].argsort()]
    cur_user = cur_user[cur_user[:, 1].argsort()]
    pre_user = np.transpose(pre_user)
    cur_user = np.transpose(cur_user)
    pre_user[2] = label
    cur_user[2] = label
    pre_user = np.transpose(pre_user)
    cur_user = np.transpose(cur_user)
    pre_user = pre_user[pre_user[:, 0].argsort()]
    cur_user = cur_user[cur_user[:, 0].argsort()]
    pre_user = np.transpose(pre_user)
    cur_user = np.transpose(cur_user)
    sp_cor = 1 - 6 * np.sum(np.square(np.subtract(pre_user[2, :], cur_user[2, :]))) / (N * (N * N - 1))
    return sp_cor

def all_data_from_table(filelog, table, column, cursor, condition=None):
    if condition is None:
        query = "SELECT " + column + " FROM " + table
    else:
        query = "SELECT " + column + " FROM " + table + ' WHERE ' + condition
    filelog.logger.debug(query)

    try:
        cursor.execute(query)
        allData = cursor.fetchall()
    except mysql.connector.Error as e:
        filelog.logger.debug(e)
        allData = []
        pass
    return allData

def main():

    # Connect to database, get all the user info
    dir = os.path.split(__file__)
    # database = 'twitter_tasmania'
    data_dir = './trust_data_updated/CoRank_map_max/'

    filelog = funclog.get_logger(fileDir=dir[0], filename=os.path.basename(__file__), loglevel='Debug', logtime=True)
    filelog.logger.info('collecting all data from files')

    # load users from file, data structure: user idx, trust value
    # load tweets need to be updated, data structure tweet idx, user idx (relation_tweets.npy)
    # load tweets that have no relation, data structure user idx, number of single tweets
    # load tweets relations, data structure: tweet idx, related tweet idx, relation type, OutDegNum for edge type from u1 to u2
    # load tweets mention user, data structure mention: mention user idx, tweet idx, user idx, OutDegNum
    # load friendship, data structure: friend idx, follower idx
    # load user stats, followers, friends, reciprocity, f_ratio, minimum value 1
    # load user tweets stats, id, relation tweets, active tweets, passive tweets, mention tweets, user mention
    # load all post tweets, tweetidx, useridx
    all_user = np.load('./data/all_user.npy')
    all_tweet = np.load('./data/relation_tweet.npy')
    single_tweet = np.load('./data/single_tweet.npy')
    tweet_relation = np.load('./data/tweet_relation.npy')
    tweet_mention = np.load('./data/tweets_mention_user.npy')
    friendship = np.load('./data/user_friends.npy')
    user_stats = np.load('./data/user_stats.npy')
    isolate_users = np.load('./data/isolate_users.npy')   # users has no friendship nor tweet connections
    tweets_stats = np.load('./data/user_tweets_stats.npy')
    all_post = np.load('./data/all_post.npy')  #eliminate the tweet that appeared in mention which mentions the owner
    user_v = np.load('./data/user_verified.npy')

    nm_iterations = 20
    delta = 0.5
    # x0_record = np.zeros(nm_iterations, 2)
    user_trust = np.ones((nm_iterations, all_user[0].__len__()))
    tweet_trust = np.ones((nm_iterations, all_tweet[0].__len__()))
    N = user_trust[0].__len__()
    M = tweet_trust[0].__len__()
    for loop in range(nm_iterations):
        if loop == 0:
            # Init trust value for users and tweets for iteration 0 then continue to iteration 1
            # user_trust[0] = [x * 0.5 for x in user_trust[0]]
            # tweet_trust[0] = [x * 0.5 for x in tweet_trust[0]]
            # u_friend = math.log(0.5+1)
            # t_user = 0.5
            user_trust[0] = user_trust[0] * 0.5
            tweet_trust[0] = tweet_trust[0] * 0.5
            u_friend = u_post = t_user = t_active = t_passive = 0.5
            continue
        user_trust_tmp = user_trust[loop-1]
        tweet_trust_tmp = tweet_trust[loop-1]
        user_trust_follower = np.zeros(user_trust_tmp.__len__())
        user_trust_friend = np.zeros(user_trust_tmp.__len__())
        # user_reciprocity = np.zeros(user_trust_tmp.__len__())
        user_trust_mention = np.zeros(user_trust_tmp.__len__())
        user_trust_post = np.zeros(user_trust_tmp.__len__())
        user_trust_friendship = np.zeros(user_trust_tmp.__len__())

        # Calculate new trust value for each user based on friendship, mention and posted tweets
        for i, user in enumerate(all_user[0]):
            isolateidx = np.where(isolate_users == user)
            # relation_tweets = tweets_stats[1][i]
            mention_tweets = tweets_stats[4][i]
            user_mention = tweets_stats[5][i]
            if isolateidx[0].size != 0:
                # filelog.logger.info('User {} is an isolate user, skip to next user'.format(user))
                user_trust_follower[i] = 0
                user_trust_friendship[i] = 0
                user_trust_friend[i] = 0
                user_trust_mention[i] = 0
                user_trust_post[i] = 0
                continue

            # ratio = user_stats[3][i]
            fo = user_stats[0][i]
            fr = user_stats[1][i]
            recp = user_stats[2][i]
            # evaluate followers' trust value to the user i, friendship[0] is friendIdx
            followerindex = np.where(friendship[0] == user)
            if followerindex[0].size == 0:
                # filelog.logger.info('User {} does not have any follower'.format(user))
                user_trust_follower[i] = 0
            else:
                followerindex = followerindex[0]
                for j in followerindex:
                    followerid = friendship[1][j]  # loop through each follower id in friendship[1] followerIdx
                    # user_reciprocity[i] = user_reciprocity[i] + friendship[2][j]
                    user_trust_follower[i] = user_trust_follower[i] + user_trust_tmp[followerid]
                # this is the new ratio for friendship
                user_trust_friendship[i] = math.log(fo * user_trust_follower[i] / (fo + fr - recp)+1)

            mentionindex = np.where(tweet_mention[0] == user)
            if mentionindex[0].size == 0:
                # filelog.logger.info('User {} has not been mentioned by any tweets'.format(user))
                user_trust_mention[i] = 0
            else:
                mentionindex = mentionindex[0]
                for j in mentionindex:
                    mentionid = tweet_mention[1][j]
                    # get outdegree number of mention edges
                    OutDegNum = tweet_mention[3][j]
                    tweet_trust_index = np.where(all_tweet[0] == mentionid)[0]
                    user_trust_mention[i] = user_trust_mention[i] + tweet_trust_tmp[tweet_trust_index]/OutDegNum
                # this is the new mention ratio
                user_trust_mention[i] = math.log((mention_tweets + 1) * user_trust_mention[i] / (user_mention +
                                                                                                   mention_tweets + 1)+1)


            # secondly, we calculate tweets posted by user that have relation with other tweets, use all_tweet data
            post_index = np.where(all_post[1] == user)
            if post_index[0].size == 0:
                # filelog.logger.info('User {} has not posted any tweet related to others'.format(user))
                post_index = 0
            else:
                post_index = post_index[0]
                for j in post_index:
                    postid = all_post[0][j]
                    tweet_trust_index = np.where(all_tweet[0] == postid)[0]
                    user_trust_post[i] = user_trust_post[i] + tweet_trust_tmp[tweet_trust_index]
            # the post tweets should be the combination of these above two parts, with weights delta1 and delta2

            user_trust_post[i] = math.log(user_trust_post[i] + 1)

            if math.isnan(user_trust_post[i]):
                filelog.logger.info('trust value is Nan, error in calculation, stop!')
                return

        # save all the data in each iteration
        # np.save(data_dir + 'trust_follower_' + str(loop) + '.npy', user_trust_follower)
        np.save(data_dir + 'trust_friendship_' + str(loop) + '.npy', user_trust_friendship)
        # np.save(data_dir + 'trust_friend_' + str(loop) + '.npy', user_trust_friend)
        np.save(data_dir + 'trust_mention_' + str(loop) + '.npy', user_trust_mention)
        np.save(data_dir + 'trust_post_' + str(loop) + '.npy', user_trust_post)

        # the final trust value of user is the weighted summation of these three parts after normalisation
        w_mention = w_post = 0.8333
        w_friendship = 0.1667

        # threshold set to consider only the users that have at least one follower and one post or mention
        # if loop == 1:
        #     u_friend = np.median(user_trust_friendship)
        threshold_u = u_friend*w_friendship
        filelog.logger.info('threshold_u = {}'.format(threshold_u))
        x1_ratio = 0.1
        x2_ratio = 0.9

        user_trust_org = np.add([w_friendship * x for x in user_trust_friendship],
                                  [w_mention * y for y in user_trust_mention])
        user_trust_org = np.add(user_trust_org, [w_post * z for z in user_trust_post])
        user_trust_tmp1 = mapping_func(user_trust_org, threshold_u, x1_ratio, x2_ratio, filelog)  # mapping
        # user_trust_tmp1 = [x / np.sum(user_trust_tmp1) for x in user_trust_tmp1]   # mapping normalisation
        user_trust_tmp2 = [x / np.max(user_trust_org) for x in user_trust_org]
        # user_trust_tmp2 = [x / np.sum(user_trust_org) for x in user_trust_org]  # normalisation
        user_trust[loop] = np.add([x *delta for x in user_trust_tmp1], [y *(1-delta) for y in user_trust_tmp2])
        # user_trust[loop] = [x/2 for x in user_trust[loop]]
        np.save(data_dir + 'user_trust_' + str(loop) + '.npy', user_trust)
        np.save(data_dir + 'user_trust_map_' + str(loop) + '.npy', user_trust_tmp1)

        # Calculate new trust value for each tweet in all_tweet
        tweet_trust_user = np.zeros(tweet_trust_tmp.__len__())
        tweet_trust_active = np.zeros(tweet_trust_tmp.__len__())
        tweet_trust_passive = np.zeros(tweet_trust_tmp.__len__())
        for i, tweet in enumerate(all_tweet[0]):
            # First, each tweet inherits its owner's trust value with weight w_owner
            owner = all_tweet[1][i]
            owner_index = np.where(all_user[0] == owner)
            tweet_trust_user[i] = user_trust_tmp[owner_index[0]]

            # Second, get related tweets' trust value with weight w_tweet
            # tweet_index_retweeted = np.where(tweet_relation[0] == tweet)  # tweet actively interact with other tweet(s)
            tweet_index_rtrp = np.where(tweet_relation[1] == tweet)  # tweet was acted by other tweet(s)

            if tweet_index_rtrp[0].size > 0:
                tweet_index_rtrp = tweet_index_rtrp[0]
                for j in tweet_index_rtrp:
                    active_tweet = tweet_relation[0][j]  # find tweet that retweet or reply to the original tweet
                    tweet_type = tweet_relation[2][j]
                    OutDegNum1 = tweet_relation[3][j]
                    w_tweet = min(tweet_type, 2) * 0.5
                    # if tweet_type == 1:
                    #     w_tweet = 0.5
                    # else:
                    #     w_tweet = 1.0
                    trust_index = np.where(all_tweet[0] == active_tweet)
                    tweet_trust_passive[i] = tweet_trust_passive[i] + w_tweet * \
                                             tweet_trust_tmp[trust_index[0]]/OutDegNum1

        np.save(data_dir + 'trust_tweet_user_' + str(loop) + '.npy', tweet_trust_user)
        np.save(data_dir + 'trust_tweet_active_' + str(loop) + '.npy', tweet_trust_active)
        np.save(data_dir + 'trust_tweet_passive_' + str(loop) + '.npy', tweet_trust_passive)

        # the final trust value of tweet is the weighted summation of these three parts after normalisation
        w_user = 0.4
        w_active = 0.6
        w_passive = 0.6
        alpha = 10
        # threshold set to consider only the tweets that have at least one active tweets

        # threshold_t = math.sqrt(w_user * t_user + w_active * t_active + w_passive*t_passive + 1) - 1
        # if loop == 1:
        #     t_user = np.median(user_trust[loop])
        threshold_t = math.log((w_user * t_user)*alpha+1)
        filelog.logger.info('threshold_t = {}'.format(threshold_t))
        x1_ratio = 0.1
        x2_ratio = 0.9

        tweet_trust_org = np.add([w_active * y for y in tweet_trust_active],
                                   [w_passive * z for z in tweet_trust_passive])
        tweet_trust_org = np.add([w_user * x for x in tweet_trust_user], tweet_trust_org)
        
        tweet_trust_org = [math.log(x*alpha+1) for x in tweet_trust_org]
        tweet_trust_tmp1 = mapping_func(tweet_trust_org, threshold_t, x1_ratio, x2_ratio, filelog)


        tweet_trust_tmp2 = [x / np.max(tweet_trust_org) for x in tweet_trust_org]
        tweet_trust[loop] = np.add([x *delta for x in tweet_trust_tmp1], [y *(1-delta) for y in tweet_trust_tmp2])


        np.save(data_dir + 'tweet_trust_' + str(loop) + '.npy', tweet_trust)

        # generate user and post value for next iteration
        tmp = [x for x in user_trust_friendship]
        u_friend = np.median(tmp)
        tmp = [x for x in user_trust_post]
        u_post = np.median(tmp)
        tmp = [x for x in user_trust[loop]]
        t_user = np.median(tmp)

        # t_passive = np.median(tmp)
        filelog.logger.info('u_friend = {}, u_post = {}, t_user = {}'.format(u_friend, u_post, t_user))
        sp_cor = spearman_rank(user_trust, loop)
        sp_cor_t = spearman_rank(tweet_trust, loop)
        filelog.logger.info('sp correlation is {} and {}'.format(sp_cor, sp_cor_t))
        if (sp_cor >= 0.9999) & (loop > 2) & (sp_cor_t >= 0.9999):
            filelog.logger.debug("--- %s seconds ---" % (time.time() - start_time))
            break

    # cursor.close()
    # conn.close()
    filelog.logger.debug("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
