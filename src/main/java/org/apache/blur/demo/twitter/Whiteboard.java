package org.apache.blur.demo.twitter;

import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

public class Whiteboard {

  /**
   * @param args
   * @throws TwitterException 
   */
  public static void main(String[] args) throws TwitterException {
    Twitter twitter = new TwitterFactory(new ConfigurationBuilder().build()).getInstance();
    OAuth2Token token = twitter.getOAuth2Token();
    System.out.println(token.getTokenType());

    try {
      Query query = new Query("Apache");
      QueryResult result;
      do {
          result = twitter.search(query);
          List<Status> tweets = result.getTweets();
          for (Status tweet : tweets) {
              System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
              
          }
      } while ((query = result.nextQuery()) != null);
      System.exit(0);
  } catch (TwitterException te) {
      te.printStackTrace();
      System.out.println("Failed to search tweets: " + te.getMessage());
      System.exit(-1);
  }
    

  }

}
