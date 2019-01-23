import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 *java版JavaRDD和JavaPairRDD之间的转换
 */
public class JavaRDDToJavaPairRDD {

    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> line = sc.parallelize(Arrays.asList("1 语文", "2 数学", "3 英语", "4 政治"));

        /**
         * 输出所有的信息
         */
        line.foreach(new VoidFunction<String>() {
            public void call(String num) throws Exception {
                System.out.println("numbers:" + num);
            }
        });

        /**
         * 将JavaRDD转换为JavaPairRDD
         */
        JavaPairRDD<String,String> prdd = line.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[0],s.split(" ")[1]);
            }
        });

        System.out.println("JavaRDD转换为JavaPairRDD---mapToPair");

        //输出信息
        prdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println(t);
            }
        });

        System.out.println("============1============");

        /**
         * 将JavaPairRDD转化为JavaRDD
         */
        JavaRDD<String> javaprdd = prdd.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> t) throws Exception {
                System.out.println(t);
                System.out.println("第一个参数是：" + t._1);
                System.out.println("第二个参数是：" + t._2);
                return t._1 + " " + t._2;
            }
        });

        System.out.println("===============2=========");

        //输出信息
        javaprdd.foreach(new VoidFunction<String>(){
            public void call(String num) throws Exception {
                System.out.println("numbers;"+num);
            }
        });


    }

}
