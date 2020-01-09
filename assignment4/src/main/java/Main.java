import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args){
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[4]")
                .appName("INDDetector")
                .getOrCreate();

        Dataset dataset = sparkSession
                .read()
                .option("delimiter", ";")
                .option("header", "true")
                .csv("../TPCH/tpch_region.csv");

        dataset.repartition(32);

        dataset.show();
    }
}
