package problem6;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class SumPriceProcessing {
    private static final String CSV_HEADER = "Transaction_date,Product,Price";

    public static void main(String[] args) {


        final  SumPriceProcessingOptions sumPriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(SumPriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(sumPriceProcessingOptions);

        pipeline.apply("Reading-Lines", TextIO.read()
                        .from(sumPriceProcessingOptions.getInputFile()))
                .apply("Filtering-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            String[] NewToken = tokens[0].split(("/"));
                            return KV.of(NewToken[1],Integer.parseInt(tokens[2]));
                        }))
                .apply("SumAggregation", Sum.integersPerKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(SalesCount -> SalesCount.getKey() + "," + SalesCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(sumPriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("Transaction_date,Sum"));

        pipeline.run();
        System.out.println("pipeline created");
    }

    public interface SumPriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/SalesJan2009.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink/Sum_price.csv")
        String getOutputFile();

        void setOutputFile(String value);
    }
}

