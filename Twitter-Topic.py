import tweepy
from google.cloud import pubsub_v1
from concurrent import futures
from google.auth import jwt
import json
import logging
import Twitter_Credentials

# Define the list of terms to listen to
lst_hashtags = ["heat"]


class GetAuthorization:
    """
    Gets Authorization from required sources and provides access to sources
    """
    def __init__(self):
        self.twitter_api = None
        self.publisher = None

    def twitter_api_connect(self, api_key):
        auth = tweepy.OAuthHandler(api_key.CONSUMER_KEY, api_key.CONSUMER_SECRET)
        auth.set_access_token(api_key.ACCESS_TOKEN, api_key.ACCESS_TOKEN_SECRET)
        self.twitter_api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

    def pubsub_connect(self, key_file):
        service_account_info = json.load(open(key_file))
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)


# Custom listener class
class StdOutListener(tweepy.streaming.StreamListener, GetAuthorization):
    """
    A listener handles tweets that are received from the stream.
    This is a basic listener that just pushes tweets to pubsub
    """

    def __init__(self, get_auth):
        super(StdOutListener, self).__init__()
        self.publisher = get_auth.publisher
        self.project_id = PROJECT_ID
        self.topic_id = TOPIC_ID
        self.publish_futures = []

    def get_callback(self, publish_future, data: str) -> callable:
        def callback(publish_future):
            try:
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error(f'Publishing {data} timed out.')

        return callback

    def publish_to_topic(self, message):
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        publish_future = self.publisher.publish(topic_path, message.encode('utf-8'))
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)

    def on_data(self, raw_data):
        logging.info(raw_data)
        self.publish_to_topic(raw_data)

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False


logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'egen-data-project-twitter'
TOPIC_ID = 'egen-data-twitter-pubsub'
if __name__ == "__main__":
    authorizer = GetAuthorization()
    authorizer.twitter_api_connect(api_key=Twitter_Credentials)
    authorizer.pubsub_connect(key_file="cloud_key_file.json")
    listner = StdOutListener(authorizer)
    stream = tweepy.Stream(auth=authorizer.twitter_api.auth, listener=listner)
    stream.filter(track=lst_hashtags, languages=['en'])
