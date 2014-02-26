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
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.QueueReader;
import org.apache.blur.server.ShardContext;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

import twitter4j.HashtagEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UserMentionEntity;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSearchQueueReader extends QueueReader {
  private static Log log = LogFactory.getLog(TwitterSearchQueueReader.class);

  public static final String TWITTER_SEARCH_CRITERIA_KEY = "twitter.search.criteria";

  private Twitter twitter;
  private String searchCriteria;
  private Query query;
  private QueryResult result;
  private String tableName;

  public TwitterSearchQueueReader(BlurIndex index, ShardContext shardContext) {
    super(index, shardContext);
    twitter = new TwitterFactory(new ConfigurationBuilder().build()).getInstance();

    try {
      OAuth2Token token = twitter.getOAuth2Token();
    } catch (TwitterException e) {
      throw new RuntimeException("Unable to connect to twitter.", e);
    }
    searchCriteria = _tableContext.getDescriptor().getTableProperties().get(TWITTER_SEARCH_CRITERIA_KEY);
    tableName = _tableContext.getTable();

    if (searchCriteria == null) {
      throw new RuntimeException("Twitter search criteria cannot be null.");
    }
    log.info("Initialized with search criteria[" + searchCriteria + "]");
  }

  private List<RowMutation> nextBatch(int count) {
    try {
      if (query == null) {
        initResult(count);
      } else {
        query = result.nextQuery();
      }

      List<Status> tweets = result.getTweets();
      List<RowMutation> mutations = new ArrayList<RowMutation>();

      for (Status tweet : tweets) {
        mutations.add(toRowMutation(tweet));
      }

      return mutations;
    } catch (Exception te) {
      throw new RuntimeException("Error retrieving tweets.", te);
    }

  }

  private RowMutation toRowMutation(Status tweet) {
    RowMutation rowMutation = new RowMutation();
    rowMutation.setRowId(tweet.getUser().getScreenName());
    rowMutation.setTable(tableName);
    rowMutation.setRowMutationType(RowMutationType.UPDATE_ROW);
    Record record = new Record();
    record.setFamily("tweets");
    record.setRecordId(tweet.getUser().getScreenName() + "-" + tweet.getId());

    record.addToColumns(new Column("message", tweet.getText()));

    for (UserMentionEntity mention : tweet.getUserMentionEntities()) {
      record.addToColumns(new Column("mentions", mention.getScreenName()));
    }

    for (HashtagEntity tag : tweet.getHashtagEntities()) {
      record.addToColumns(new Column("hashtags", tag.getText()));
    }
    rowMutation.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));

    log.trace(rowMutation);

    return rowMutation;
  }

  private void initResult(int count) throws TwitterException, InterruptedException {
    log.info("Initializing result for query[" + searchCriteria + "] with count[" + count + "]");
    query = new Query(searchCriteria);
    query.setCount(count);
    result = twitter.search(query);
  }

  @Override
  public void failure() {
    log.error("Failed to index.");

  }

  @Override
  public void success() {
    log.info("Wahoo!");

  }

  @Override
  public void take(List<RowMutation> rowMutation, int count) {
    rowMutation.addAll(nextBatch(count));

  }

}
