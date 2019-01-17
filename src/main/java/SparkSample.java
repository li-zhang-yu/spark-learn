import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * GroupByKey的使用
 */
public class SparkSample {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Spark_GroupByKey_Sample").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        List<Integer> data = Arrays.asList(1,1,2,2,1);
        JavaRDD<Integer> distData = context.parallelize(data);

        JavaPairRDD<Integer,Integer> firstRDD = distData.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,integer*integer);
            }
        });

        JavaPairRDD<Integer,Iterable<Integer>> secondRDD = firstRDD.groupByKey();

        List<Tuple2<Integer,String>> reslist = secondRDD.map(new Function<Tuple2<Integer, Iterable<Integer>>, Tuple2<Integer, String>>(){
            public Tuple2<Integer, String> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
                int key = t._1();
                StringBuffer sb = new StringBuffer();
                Iterable<Integer> iter = t._2();
                for(Integer integer : iter){
                    sb.append(integer).append(" ");
                }
                return new Tuple2<Integer, String>(key,sb.toString().trim());
            }
        }).collect();

        for(Tuple2<Integer,String> str : reslist){
            System.out.println(str._1() + "\t" + str._2() );
        }

        context.stop();

    }
}
