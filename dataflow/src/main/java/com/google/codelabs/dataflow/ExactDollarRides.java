/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.codelabs.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.codelabs.dataflow.utils.RideToTableRow;
import com.google.codelabs.dataflow.utils.TableRowToJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.codelabs.dataflow.utils.CustomPipelineOptions;
import java.util.Date;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Dataflow command-line options must be specified:
//   --project=<your project ID>
//   --sinkProject=<your project ID>
//   --stagingLocation=gs://<your staging bucket>
//   --runner=DataflowPipelineRunner
//   --streaming=true
//   --numWorkers=3
//   --zone=<your compute zone>
// You can launch the pipeline from the command line using:
// mvn exec:java -Dexec.mainClass="com.google.codelabs.dataflow.ExactDollarRides" -e -Dexec.args="<your arguments>"

@SuppressWarnings("serial")
public class ExactDollarRides {
  private static final Logger LOG = LoggerFactory.getLogger(ExactDollarRides.class);

  // ride format from PubSub
  // {
  // "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
  // "latitude":40.66684000000033,
  // "longitude":-73.83933000000202,
  // "timestamp":"2016-08-31T11:04:02.025396463-04:00",
  // "meter_reading":14.270274,
  // "meter_increment":0.019336415,
  // "ride_status":"enroute",
  // "passenger_count":2
  // }

  private static class TransformRides extends DoFn<Double, TableRow> {
    TransformRides() {}

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      Double dollars = c.element();
      TableRow r = new TableRow();
      r.set("dollar_turnover", dollars);
      // the timing can be:
      // EARLY: the dollar amount is not yet final
      // ON_TIME: dataflow thinks the dollar amount is final but late data are still possible
      // LATE: late data has arrived
      r.set("dollar_timing", c.pane().getTiming()); // EARLY, ON_TIME or LATE
      r.set("dollar_window", window.maxTimestamp().getMillis() / 1000.0 / 60.0); // timestamp in fractional minutes

      LOG.info("Outputting $ value {}} at {} with marker {} for window {}",
        dollars.toString(), new Date().getTime(), c.pane().getTiming().toString(), window.hashCode());
      c.output(r);
    }
  }

  public static void main(String[] args) {
    CustomPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("read from PubSub", PubsubIO.readStrings()
        .fromTopic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
        .withTimestampAttribute("ts"))

     .apply("convert to tablerow", ParDo.of(new RideToTableRow()))

     .apply("extract dollars",
        MapElements.into(TypeDescriptor.of(Double.class)).via((TableRow x) -> Double.parseDouble(x.get("meter_increment").toString())))

     .apply("fixed window", Window.<Double>into(FixedWindows.of(Duration.standardMinutes(1)))
             .triggering(
          AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)))
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
          .accumulatingFiredPanes()
          .withAllowedLateness(Duration.standardMinutes(5)))

     .apply("sum whole window", Sum.doublesGlobally().withoutDefaults())
     .apply("format rides", ParDo.of(new TransformRides()))
     .apply("convert to JSON", ParDo.of(new TableRowToJson()))
     .apply("WriteToPubsub", PubsubIO.writeStrings()
        .to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())));
    p.run();
  }
}
