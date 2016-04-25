package com.twitter.storm;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamServer {

	public static void main(String[] args) {

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey(TwitterStreamConsumer.consumerKey);
		cb.setOAuthConsumerSecret(TwitterStreamConsumer.consumerSecret);
		cb.setOAuthAccessToken(TwitterStreamConsumer.accessTokens);
		cb.setOAuthAccessTokenSecret(TwitterStreamConsumer.accessSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		StatusListener listener = new StatusListener() {

			public void onException(Exception arg0) {
				// TODO Auto-generated method stub

			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub

			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

			public void onStatus(Status status) {
				twitter4j.User user = status.getUser();

				// gets Username
				String username = status.getUser().getScreenName();
				System.out.println(username);
				String profileLocation = user.getLocation();
				System.out.println(profileLocation);
				long tweetId = status.getId();
				System.out.println(tweetId);
				String content = status.getText();
				System.out.println(content + "\n");

			}

			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub

			}
		};
		FilterQuery fq = new FilterQuery();

		String keywords[] = { "ireland" };

		fq.track(keywords);

		twitterStream.addListener(listener);
		twitterStream.filter(fq);

		final TwitterStreamConsumer streamConsumer = new TwitterStreamConsumer();
		streamConsumer.start();
	}
}
