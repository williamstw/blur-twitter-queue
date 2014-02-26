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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.manager.indexserver.DefaultBlurIndexWarmup;
import org.apache.blur.manager.writer.BlurIndexCloser;
import org.apache.blur.manager.writer.BlurIndexSimpleWriter;
import org.apache.blur.manager.writer.SharedMergeScheduler;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TwitterSearchQueueReaderTest {
  private static final int TOTAL_TWEETS_FOR_TESTS = 100;
  private static final String TEST_TABLE = "tweets-table";
  private static final File TMPDIR = new File("./target/tmp");

  private BlurIndexSimpleWriter _writer;
  private Random random = new Random();
  private ExecutorService _service;
  private File _base;
  private Configuration _configuration;

  private SharedMergeScheduler _mergeScheduler;
  private String uuid;
  private BlurIndexCloser _closer;
  private DefaultBlurIndexWarmup _indexWarmup;

  @Before
  public void setup() throws IOException {
    // TableContext.clear();
    _base = new File(TMPDIR, TwitterSearchQueueReaderTest.class.getSimpleName());
    rmr(_base);
    _base.mkdirs();

    _mergeScheduler = new SharedMergeScheduler(1);

    _configuration = new Configuration();
    _service = Executors.newThreadPool("test", 10);
    _closer = new BlurIndexCloser();
    _indexWarmup = new DefaultBlurIndexWarmup(1000000);
  }

  private void setupWriter(Configuration configuration) throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(TEST_TABLE);
    /*
     * if reload is set to true...we create a new writer instance pointing to
     * the same location as the old one..... so previous writer instances should
     * be closed
     */

    uuid = UUID.randomUUID().toString();

    tableDescriptor.setTableUri(new File(_base, "table-store-" + uuid).toURI().toString());
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_SHARD_INDEX_QUEUE_READER_CLASS,
        TwitterSearchQueueReader.class.getName());
    tableDescriptor.putToTableProperties(TwitterSearchQueueReader.TWITTER_SEARCH_CRITERIA_KEY, "Apache");
    TableContext tableContext = TableContext.create(tableDescriptor);

    File path = new File(_base, "index_" + uuid);
    path.mkdirs();
    FSDirectory directory = FSDirectory.open(path);
    ShardContext shardContext = ShardContext.create(tableContext, "test-shard-" + uuid);
    _writer = new BlurIndexSimpleWriter(shardContext, directory, _mergeScheduler, _service, _closer, _indexWarmup);
  }

  @After
  public void tearDown() throws IOException {
    _writer.close();
    _mergeScheduler.close();
    _service.shutdownNow();
    rmr(_base);
  }

  private void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  @Test
  public void testQueueReader() throws IOException, InterruptedException {
    System.out.println(_configuration.get(BlurConstants.BLUR_SHARD_INDEX_QUEUE_READER_CLASS));
    setupWriter(_configuration);
    long maxWait = TimeUnit.SECONDS.toMillis(20);
    long start = System.currentTimeMillis();
    while (true) {
      if (_writer.getIndexSearcher().getIndexReader().numDocs() >= TOTAL_TWEETS_FOR_TESTS) {
        break;
      }
      if ((System.currentTimeMillis() - start) > maxWait) {
        fail("Took too long to get tweets, somethings wrong.");
      }
      Thread.sleep(100);
    }

  }

}
