import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * flatmap的使用
 */
public class SparkFlatMap {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark_FlatMap_Sample").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD lines = context.parallelize(Arrays.asList("helloworld","hi"));
//        JavaRDD words = lines.flatMap(
//                new FlatMapFunction() {
//                    public Iterable call(Object o) throws Exception {
//                        return Arrays.asList(o.split(" "));;
//                    }
//                }
//        );
    }
}
