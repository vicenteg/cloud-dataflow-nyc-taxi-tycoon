package com.google.codelabs.dataflow.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;

public class RideToTableRow extends DoFn<String, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        Gson gson = new Gson();
        TableRow row = new TableRow();
        String jsonPayload = c.element();
        row = gson.fromJson(jsonPayload, TableRow.class);
        c.output(row);
    }
}
