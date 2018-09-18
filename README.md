# Twitter_Tas_dataset
This is the dataset used for our research project: Trust Evaluation on Twitter. 

The proposed Co-Rank method and detailed experimental results can be found in our paper: 

## "CoRank A Coupled Dual Networks Approach to Trust Evaluation on Twitter" (WISE 2018).

The data set is designed for research purpose only. There are four files storing the Twitter data crawled from Twitter using its official REST API.

## 'tas_users' 
This file stores all the users in the Tas dataset. We have given each user a user index and the UserID is their original user ID collected from Twitter. These users are crawled based on the 'location' attribute in their profile. They are considered all from the Tasmania Twitter Community.

first column: User_index | second column: UserID

for example: "7232"	 "1076277643". 


## 'twitter_tweets_all_user_posted'
This file stores all the tweets posted by the users in the Tas dataset. These tweets are crawled from Twitter platform using RestAPI. Please note that, as restricted by Twitter, only a maximum of 3,200 most recent tweets of each user are allowed to be downloaded.

first column: TweetID | second column: Tweet_index | third column: User_index

for example: "100000183710007296"	"0"	"12423"

## 'twitter_follower_friend_index'
This file stores the follow relations between users. Note that, all the numbers are indicating user_index numbers.

first column: Follower_index | second column: Friend_index

for example: "0"	"180".  User_index 0 is following User_index 180.

## 'twitter_tweets_relation_index'


## 'twitter_user_mention_index'


*Note that, due to the request from Twitter, we can not publish the content of Twitter.

# References
If you use this data set for research, please cite one of the following papers:
