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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.codelabs.dataflow.utils.CustomPipelineOptions;
import com.google.codelabs.dataflow.utils.RidePoint;
import org.apache.beam.sdk.values.TypeDescriptors;
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
// mvn exec:java -Dexec.mainClass="com.google.codelabs.dataflow.LatestRides" -e -Dexec.args="<your arguments>"

@SuppressWarnings("serial")
public class LatestRides {
  private static final Logger LOG = LoggerFactory.getLogger(LatestRides.class);

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

  private static class LatestPointCombine extends Combine.CombineFn<TableRow, RidePoint, TableRow> {

    public RidePoint createAccumulator() {
      return new RidePoint();
    }

    public RidePoint addInput(RidePoint latest, TableRow input) {
      RidePoint newPoint = new RidePoint(input);
      if (latest.rideId == null || newPoint.timestamp > latest.timestamp) return newPoint;
      else return latest;
    }

    public RidePoint mergeAccumulators(Iterable<RidePoint> latestList) {
      RidePoint merged = createAccumulator();
      for (RidePoint latest : latestList) {
        if (merged.rideId == null || latest.timestamp > merged.timestamp)
          merged = new RidePoint(latest);
      }
      return merged;
    }

    public TableRow extractOutput(RidePoint latest) {
      return latest.toTableRow();
    }
  }

  public static void main(String[] args) {
    CustomPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply("read from PubSub", PubsubIO.readStrings()
        .fromTopic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
        .withTimestampAttribute("ts"))
     .apply("convert to TableRow", ParDo.of(new RideToTableRow()))
     .apply("key rides by rideid",
        MapElements.into(TypeDescriptors.kvs(TypeDescriptor.of(String.class), TypeDescriptor.of(TableRow.class)))
            .via((TableRow ride) -> KV.of(ride.get("ride_id").toString(), ride)))

     .apply("session windows on rides with early firings",
        Window.<KV<String, TableRow>>into(
          Sessions.withGapDuration(Duration.standardMinutes(60)))
            .triggering(
              AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(2000))))
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO))

     .apply("group ride points on same ride", Combine.perKey(new LatestPointCombine()))

     .apply("discard key",
        MapElements.into(TypeDescriptor.of(TableRow.class)).via((KV<String, TableRow> a) -> a.getValue()))
     .apply("convert to JSON", ParDo.of(new TableRowToJson()))
     .apply("WriteToPubsub", PubsubIO.writeStrings()
        .to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())));
    p.run();
  }
}
