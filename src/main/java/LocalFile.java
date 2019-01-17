import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 统计文件总字数
 */
public class LocalFile {

    public static void main(String[] args) {
        //创建SparkConf
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        //创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("F://test.txt");

        /**
         * 统计文本文件内的字数
         */
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("文件总字数是：" + count);

        //关闭JavaSparkContext
        sc.close();

    }

}
