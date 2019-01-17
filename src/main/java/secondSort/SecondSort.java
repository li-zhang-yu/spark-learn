package secondSort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondSort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String>  lines = sc.textFile("F://test.txt");

        JavaPairRDD<SecondarySortKey,String> paris = lines.mapToPair(
                new PairFunction<String, SecondarySortKey, String>() {
                    public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                        String[] lineSplited = line.split(" ");
                        SecondarySortKey key = new SecondarySortKey(
                                Integer.valueOf(lineSplited[0]),
                                Integer.valueOf(lineSplited[1])
                        );
                        return new Tuple2<SecondarySortKey, String>(key,line);
                    }
                }
        );

        JavaPairRDD<SecondarySortKey,String> sortedPairs = paris.sortByKey();

        JavaRDD<String> sortedLines = sortedPairs.map(
                new Function<Tuple2<SecondarySortKey, String>, String>() {
                    public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                        return v1._2;
                    }
                }
        );

        sortedLines.foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();
        
    }
}
