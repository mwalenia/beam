/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.pubsub;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.Subscription;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class PubsubIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubIOIT.class);

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static final String READ_TIME_METRIC_NAME = "read_time";

  private static final String WRITE_TIME_METRIC_NAME = "write_time";

  private static final String RUN_TIME_METRIC_NAME = "run_time";

  private static final String NAMESPACE = PubsubIOIT.class.getName();

  private static final String TEST_ID = UUID.randomUUID().toString();

  private static final String TIMESTAMP = Instant.now().toString();

  private static SyntheticSourceOptions sourceOptions = null;
  private static Options testOptions = null;
  private static String expectedHash = "";
  private static Long windowDuration = 10L;
  private static SubscriptionAdminClient subscriptionAdminClient;
  private static TopicAdminClient topicAdminClient;
  private static String subscriptionName;

  @BeforeClass
  public static void setup() throws IOException {
    testOptions = IOITHelper.readIOTestPipelineOptions(Options.class);
    sourceOptions =
        SyntheticOptions.fromJsonString(
            testOptions.getSourceOptions(), SyntheticSourceOptions.class);
    subscriptionAdminClient = SubscriptionAdminClient.create();
    topicAdminClient = TopicAdminClient.create();
    try {
      topicAdminClient.getTopic(testOptions.getPubsubTopic());
    } catch (NotFoundException e){
      topicAdminClient.createTopic(testOptions.getPubsubTopic());
    }
    subscriptionName = "projects/" + testOptions.getProject() + "/subscriptions/" + "PubsubIOITSubscription" + UUID.randomUUID().toString().substring(0, 5);
    Subscription sub = Subscription.newBuilder().setName(subscriptionName)
        .setAckDeadlineSeconds(10)
        .setTopic(testOptions.getPubsubTopic())
        .build();
    subscriptionAdminClient.createSubscription(sub);
    expectedHash = "";
    windowDuration = 10L;
  }

  @AfterClass
  public static void teardown() {
    subscriptionAdminClient.deleteSubscription(subscriptionName);
    topicAdminClient.deleteTopic(testOptions.getPubsubTopic());
    topicAdminClient.close();
    subscriptionAdminClient.close();
  }

  @Test
  public void testPubSubWriteAndRead() throws IOException{
    writePipeline
        .apply("Generate records", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Map records to Pubsub Message", MapElements.via(new KVToPubSubMapper()))
        .apply("Measure write time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC_NAME)))
        .apply(
            "Write messages to Pubsub", PubsubIO.writeMessages().to(testOptions.getPubsubTopic()));
    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();
    LOG.warn("Finished writing");
    PCollection<String> hashedResults =
        readPipeline
            .apply(
                "Read messages from Pubsub",
                PubsubIO.readMessages().fromSubscription(subscriptionName))
            .apply(
                "Measure read time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)))
            .apply("Map Pubsub messages to KV", MapElements.via(new PubSubToStringMapper()))
            .apply("Apply windowing", Window.into(FixedWindows.of(Duration.standardSeconds(windowDuration))))
            .apply("Calculate hash", Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.thatSingleton(hashedResults).isEqualTo(expectedHash);
    PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState = readResult.waitUntilFinish(Duration.standardMinutes(1));
    LOG.warn("Finished waiting.");
    cancelIfTimeouted(readResult, readState);

    Set<NamedTestResult> namedTestResults = readMetrics(writeResult, readResult);
    IOITMetrics.publish(
        testOptions.getBigQueryDataset(), testOptions.getBigQueryTable(), namedTestResults);
  }

  private Set<NamedTestResult> readMetrics(PipelineResult writeResult, PipelineResult readResult) {
    BiFunction<MetricsReader, String, NamedTestResult> supplier =
        (reader, metricName) -> {
          long start = reader.getStartTimeMetric(metricName);
          long end = reader.getEndTimeMetric(metricName);
          return NamedTestResult.create(TEST_ID, TIMESTAMP, metricName, (end - start) / 1e3);
        };

    NamedTestResult writeTime =
        supplier.apply(new MetricsReader(writeResult, NAMESPACE), WRITE_TIME_METRIC_NAME);
    NamedTestResult readTime =
        supplier.apply(new MetricsReader(readResult, NAMESPACE), READ_TIME_METRIC_NAME);
    NamedTestResult runTime =
        NamedTestResult.create(
            TEST_ID, TIMESTAMP, RUN_TIME_METRIC_NAME, writeTime.getValue() + readTime.getValue());

    return ImmutableSet.of(readTime, writeTime, runTime);
  }

  private static class KVToPubSubMapper extends SimpleFunction<KV<byte[], byte[]>, PubsubMessage> {

    @Override
    public PubsubMessage apply(KV<byte[], byte[]> input) {
      return new PubsubMessage(input.getValue(), Collections.EMPTY_MAP);
    }
  }

  private void cancelIfTimeouted(PipelineResult readResult, PipelineResult.State readState)
      throws IOException {
    LOG.warn("Job state is {}", readState.name());
    if (readState == null) {
      LOG.warn("Cancelling the job after timeout.");
      readResult.cancel();
    }
  }

  private static class PubSubToStringMapper extends SimpleFunction<PubsubMessage, String> {
    @Override
    public String apply(PubsubMessage input) {
      return Arrays.toString(input.getPayload());
    }
  }

  public interface Options extends IOTestPipelineOptions, PubsubOptions {
    @Validation.Required
    @Description("Synthetic source options")
    String getSourceOptions();

    void setSourceOptions(String options);

    @Validation.Required
    @Description("Pubsub topic")
    String getPubsubTopic();

    void setPubsubTopic(String topic);
  }
}
