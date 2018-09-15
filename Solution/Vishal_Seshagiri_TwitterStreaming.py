import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import re
import json
import random
from collections import Counter

# consumer_key = "JmHjHxQMPZQSOvX1G0R3KxA6f"
# consumer_secret = "QjQLo0wIqSoEQFDmTqhwstBALPQ6zOf1zBu6KokiaOaciexgdY"
# access_token = "967948938-FUojzp6q4L4DlOFPzVrhEgRHclizuHpIyJfxX22b" 
# access_token_secret = "h53tSbTvP9VeZdwTqWmFiwXpuawC7oWpLraPhwPFjEX5w"

consumer_key = "AN2meAGw1ml4R5viiFEmO9GOK"
consumer_secret = "EdlYhTEe1voXeOCwgzoimI7xBL3wsbpSmTBBppNxv6ph75xMIM"
access_token = "967948938-DIlY8bnM1qk8ygGpZJEwjZBcGhuky5yXvI6ueu4h"
access_token_secret = "nlmpzBsQwHG7V6eN3JJ68jfprtLsC90d8xYniQUQJ1WVs"

class Listener(StreamListener):
    
	def on_data(self, data):

		global tweet_count
		global window
		global avg_tweet_length
		
		data = json.loads(data)
		
		full_text_retweeted = data.get("retweeted_status")
		
		if None != full_text_retweeted and full_text_retweeted.get("truncated") != False:
			tweet_text = full_text_retweeted.get("extended_tweet").get("full_text")
		else:
			if None != data.get("extended_tweet"):
				tweet_text = data.get("extended_tweet").get("full_text")
			else:
				tweet_text = data.get("text")

		tweet_count += 1
		
		re_hashtag_pattern = "#(\w+)"

		hash_tags_in_current_tweet = re.findall(re_hashtag_pattern, tweet_text)

		if tweet_count <= 100:
			window.append([hash_tags_in_current_tweet, len(tweet_text)])
		else:
			# if 101 in random.sample(range(1, tweet_count), 100):
			if random.randint(1, tweet_count) < 101:
				index_to_replace = random.randint(0, 99)
				window[index_to_replace] = [hash_tags_in_current_tweet, len(tweet_text)]

								
		hashtag_list = []
		tweet_length_sum = 0
		for tweet in window:
			hashtag_list.extend(tweet[0])
			tweet_length_sum += tweet[1]
		
		avg_tweet_length = float(tweet_length_sum) / len(window)

		res = Counter(hashtag_list)
		top5 = "\n".join(["{}:{}".format(i[0], i[1]) for i in res.most_common()[:5]])
		
		if tweet_count > 100:
			print_string = "The number of the twitter from beginning: {tweet_count}\n"\
											"Top 5 hot hashtags:\n{top5}\n"\
											"The average length of the twitter is: {avg_tweet_length}\n"\
											.format(tweet_count=tweet_count, top5=top5, avg_tweet_length=avg_tweet_length)
			
			print(print_string)
		return True
	
	def on_error(self, status):
		print(status)

if __name__=="__main__":

	auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)

	window = []
	tweet_count = 0
	avg_tweet_length = 0.0

	# twitter_stream.disconnect()
	twitter_stream = Stream(auth, Listener(), tweet_mode="extended")
	twitter_stream.filter(track=["#"])