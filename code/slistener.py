# http://nbviewer.ipython.org/github/raynach/hse-twitter/blob/master/docs/Collecting%20Twitter%20data%20from%20the%20API%20with%20Python.ipynb
# Collecting data is pretty straightforward with tweepy.
# The first thing to do is to create an instance of a tweepy StreamListener to handle the incoming data.
# The way that I have mine set up is that I start a new file for every 20,000 tweets, tagged with a prefix and a timestamp.
# I also keep another file open for the list of status IDs that have been deleted, which are handled differently than other tweet data.
# I call this file slistener.py. You should have a copy of it.

from tweepy import StreamListener
from tweepy.api import API
import json
import time
import sys
import os

class SListener(StreamListener):

    def __init__(self, api = None, fprefix = 'streamer', location = None):
        self.baseDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.api = api or API()
        self.counter = 0
        self.fprefix = fprefix
        self.location = location
        self.output  = open(self.baseDir + '/twitter_data/streaming_data/' + self.location + '/' + self.fprefix + '.'
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        self.delout  = open((self.baseDir + '/twitter_data/streaming_data/delete.txt'), 'a')

    def on_data(self, data):

        if 'in_reply_to_status_id' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            # print warning['message']
            return False

    def on_status(self, status):
        self.output.write(status + "\n")

        self.counter += 1

        if self.counter >= 3000:
            self.output.close()
            self.output = open(self.baseDir + '/twitter_data/streaming_data/' + self.location + '/' + self.fprefix + '.'
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.counter = 0
        return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return



