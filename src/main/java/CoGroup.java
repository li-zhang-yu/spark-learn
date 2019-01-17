import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * cogroup的使用
 *
 */
public class CoGroup {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark WordCount!").setMaster("local");
        JavaSparkContext sContext = new JavaSparkContext(conf);
        List<Tuple2<Integer,String>> namesLists = Arrays.asList(
                new Tuple2<Integer, String>(1,"spark"),
                new Tuple2<Integer, String>(3,"Tachyon"),
                new Tuple2<Integer, String>(4,"Sqoop"),
                new Tuple2<Integer, String>(2,"hadoop"),
                new Tuple2<Integer, String>(2,"hadoop2")
        );

        List<Tuple2<Integer,Integer>> scoreLists = Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(3,70),
                new Tuple2<Integer, Integer>(3,77),
                new Tuple2<Integer, Integer>(2,90),
                new Tuple2<Integer, Integer>(2,80)
        );

        JavaPairRDD<Integer,String> names = sContext.parallelizePairs(namesLists);
        JavaPairRDD<Integer,Integer> scores = sContext.parallelizePairs(scoreLists);

        JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>> nameScores = names.cogroup(scores);

        nameScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            private static final long serialVersionUID = 1L;
            int i = 1;
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                String string = "ID:" + t._1 + " , " + "Name:" + t._2._1 + " , " + "Score:" + t._2._2;
                string+="   count:" + i;
                System.out.println(string);
                i++;
            }
        });

        sContext.close();
    }

}
