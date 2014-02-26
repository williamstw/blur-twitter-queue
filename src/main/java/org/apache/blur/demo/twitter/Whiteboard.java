package org.apache.blur.demo.twitter;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
