package com.google.codelabs.dataflow.utils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;

public class TableRowToJson extends DoFn<TableRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        Gson gson = new Gson();
        TableRow tableRow = c.element();

        String json = gson.toJson(tableRow);
        c.output(json);
    }
}
