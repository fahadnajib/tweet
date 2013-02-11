package com.xceed.ventures;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TweetOConnect {
	
	private final Logger logger = Logger.getLogger(TweetOConnect.class.getName());
	
	/**
	 * Connect to Twitter Using OAuth authentication
	 * @return
	 */
	protected Twitter connect(){
		Twitter twitter = null;
		try{
			ConfigurationBuilder cb = new ConfigurationBuilder();
			  cb.setOAuthConsumerKey("TkaqsWAcTmfXPB7eLtFVuQ"); // INPUT CREDENTIALS HERE!!
			  cb.setOAuthConsumerSecret("QERAXM0GjVnAKN8un1tBjD2H7XW42wBWMRm6RKuzg");
			  cb.setOAuthAccessToken("18968449-sOu17lvpjU9U26EtWlz5T3koBDStx33bN2MeU8");
			  cb.setOAuthAccessTokenSecret("ALOU4YZvlQ5bXRLBJkair4thKlTV6xG4PHjfQyMI0");
			  cb.setDebugEnabled(true);
			  cb.setJSONStoreEnabled(true);
			  
			  twitter = new TwitterFactory(cb.build()).getInstance();
			 
//			  TwitterFactory factory = new TwitterFactory(new
//					 ConfigurationBuilder()
//					                 .setJSONStoreEnabled(true)
//					                 .build());
			//Twitter tw = factory.getInstance(); 
			//twitter = new TwitterFactory().getInstance();
			try{
				RequestToken requestToken = twitter.getOAuthRequestToken();
				AccessToken accessToken = null;
				while (null == accessToken) {
					logger.fine("Open the following URL and grant access to your account:");
					logger.fine(requestToken.getAuthorizationURL());
					try {
						accessToken = twitter.getOAuthAccessToken(requestToken);
					} catch (TwitterException te) {
						if (401 == te.getStatusCode()) {
							logger.severe("Unable to get the access token.");
						} else {
							te.printStackTrace();
						}
					}
				}
				logger.severe("Got access token.");
				logger.info("Access token: " + accessToken.getToken());
				logger.info("Access token secret: " + accessToken.getTokenSecret());
				} catch (IllegalStateException ie) {
					// 	access token is already available, or consumer key/secret is not set.
					if (!twitter.getAuthorization().isEnabled()) {
						logger.severe("OAuth consumer key/secret is not set.");
						return null;
					}
				}
			//Status status = twitter.updateStatus(message);
			//logger.info("Successfully updated the status to [" + status.getText() + "].");
			} catch (TwitterException te) {
				te.printStackTrace();
				logger.severe("Failed to get timeline: " + te.getMessage());
			}
			return twitter;
		}
	
	/**
	 * Read tweets from tweeter
	 * @param twitter
	 * @param page
	 * @param numberOfTweets
	 * @return a List containing tweets
	 */
	protected List read(Twitter twitter,int page,int numberOfTweets){
		
		List jsonTweets = new ArrayList(numberOfTweets);
		try{
			List<Status> statuses = twitter.getHomeTimeline(new Paging(page,numberOfTweets));
			
			System.out.println("Showing home timeline.");int i=1;
			for (Status status : statuses) {
				String rawJSON = DataObjectFactory.getRawJSON(status);
				System.out.println(""+i++ +"JSON: "+rawJSON);
				//System.out.println(i++ + ". "+status.getUser().getName() + ":" +
			      //                   status.getText()+" Geo Location "+status.getGeoLocation()
			        //                 +" source"+status.getSource());
				jsonTweets.add(rawJSON);
			}
		} catch (TwitterException te) {
			te.printStackTrace();
			logger.severe("Failed to get timeline: " + te.getMessage());
			return null;
		}
		return jsonTweets;
	}
	
}
