import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * join方法得使用
 */
public class SparkRDDDemo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        //FirstRDD
        JavaPairRDD<Integer,Integer> firstRDD = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer num) throws Exception {
                return new Tuple2<Integer, Integer>(num,num*num);
            }
        });

        //SecondRDD
        JavaPairRDD<Integer,String> secondRDD = rdd.mapToPair(new PairFunction<Integer, Integer, String>() {
            public Tuple2<Integer, String> call(Integer num) throws Exception {
                return new Tuple2<Integer, String>(num,String.valueOf((char) (64 + num*num)));
            }
        });

        JavaPairRDD<Integer,Tuple2<Integer,String>> joinRDD = firstRDD.join(secondRDD);

        JavaRDD<String> res = joinRDD.map(new Function<Tuple2<Integer, Tuple2<Integer, String>>, String>() {
            public String call(Tuple2<Integer, Tuple2<Integer, String>> integerTuple2Tuple2) throws Exception {
                int key = integerTuple2Tuple2._1();
                int value1 = integerTuple2Tuple2._2()._1();
                String value2 = integerTuple2Tuple2._2()._2();
                return "<" + key + ",<" + value1 + "," + value2 + ">>";
            }
        });

        List<String> resList = res.collect();
        for(String str : resList)
            System.out.println(str);

        sc.stop();
    }

}
