package problem6;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


final class ApacheBeamTextIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(problem6.ApacheBeamTextIO.class);

    private static final String CSV_HEADER = "Transaction_date,Price";

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read-Lines", TextIO.read().from("src/main/resources/source/SalesJan2009.csv"))
                .apply("Filter-Header", ParDo.of(new problem6.ApacheBeamTextIO.FilterHeaderFn(CSV_HEADER)))
                .apply("Map",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                .via((String line) -> {
                                    String[] tokens = line.split(",");
                                    return KV.of(tokens[0], Double.parseDouble(tokens[2]));
                                }))
                .apply("count-aggregation", Count.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + "," + typeCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to("src/main/resources/sink/sales")
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("date,total_sale"));

        LOGGER.info("Executing pipeline");
        pipeline.run();
    }

    //TODO: keep it in another class not as inner class.
    private static class FilterHeaderFn extends DoFn<String, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(ApacheBeamTextIO.class);

        private final String header;

        FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext processContext) {
            String row = processContext.element();
            if (!row.isEmpty() && !row.contains(header))
                processContext.output(row);
            else
                LOGGER.info("Filtered out the header of the csv file: [{}]", row);

        }
    }
}