import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * map的使用
 */
public class SparkMap {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark_Map_Sample").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD rdd = context.parallelize(Arrays.asList(1,3,5,7));
        JavaRDD result = rdd.map(
                new Function() {
                    public Object call(Object o) throws Exception {
                        return o;
                    }
                }
        );
        System.out.println(StringUtils.join(result.collect(),","));
    }



}
