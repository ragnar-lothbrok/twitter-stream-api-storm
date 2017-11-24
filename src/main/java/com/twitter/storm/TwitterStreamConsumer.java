package com.twitter.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

public class TwitterStreamConsumer extends Thread {

	public static String accessTokens = "159371302-Kd442ysh0Kn74jRuMKB6H9ZOag5JqNotn4It4cfp";
	public static String accessSecret = "hFZRfRaCG0pXAuTVLXLTClxVFgOUwhQJNLWCMTNKwyXpL";
	public static String consumerKey = "RvOF6790fpuhOtt00vJC3h8MQ";
	public static String consumerSecret = "KW7ys6A7hnKGIn3bz7BggxbeWCubxgGhDgKfso7Ogrv8Wx8DKE";

	private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/filter.json";
	private String latestTweet;
	private int tweetCount;

	public String getLatestTweet() {
		return latestTweet;
	}

	public int getTweetCount() {
		return tweetCount;
	}

	public void run() {
		try {
			// Enter your consumer key and secret below
			OAuthService service = new ServiceBuilder().provider(TwitterApi.class).apiKey(consumerKey).apiSecret(consumerSecret).build();

			// Set your access token
			Token accessToken = new Token(accessTokens, accessSecret);

			// Let's generate the request
			System.out.println("Connecting to Twitter Public Stream");
			OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
			request.addHeader("version", "HTTP/1.1");
			request.addHeader("host", "stream.twitter.com");
			request.setConnectionKeepAlive(true);
			request.addHeader("user-agent", "Twitter Stream Reader");
			/**
			 * Keywords which we would like to track
			 */
			request.addBodyParameter("track", "#AAPEducationSham");
			service.signRequest(accessToken, request);
			Response response = request.send();

			// Create a reader to read Twitter's stream
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

			String line;
			while ((line = reader.readLine()) != null) {
				latestTweet = line;
				tweetCount++;
				System.out.println(line);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

	}
}