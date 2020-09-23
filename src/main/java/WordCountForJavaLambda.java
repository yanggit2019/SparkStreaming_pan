//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import scala.Tuple2;
//
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//
//public class WordCountForJavaLambda {
//    public static void main(String[] args) {
//        // 代表集群模式：local : 本地一台机器
//        // local[*] : 电脑的CPU核数
//        // local[N] : 程序需要配置的CPU核
//        // setAppName：应用程序运行的任务名称
//        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcountforjava");
//
//        // 创建java 版 SparkContext对象，就相当于spark的引擎
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//
//        // 创建rdd
//        // 把输入目录的文件转成rdd
//        // JavaRDD<String> 其中的String 是 每一行的数据
//        JavaRDD<String> rdd = jsc.textFile("C:\\Users\\My\\Desktop\\spark\\input");
//
//        // "aa bb aa" --> Array(aa, bb, aa)
//        // FlatMapFunction<String,String>
//        // ->左面：入参，代表每行的数据
//        // ->右面：返回类型，代表每个单词集合
//        JavaRDD<String> flatMapRdd = rdd.flatMap(f ->{
//                String[] arr = f.split(" ");
//                // 每行单词组成的集合
//                Iterator<String> iterator = Arrays.asList(arr).iterator();
//                return iterator;
//
//        });
//
//        //  Array(aa, bb, aa) --> Array((aa,1), (bb,1), (aa,1))
//        // Function<String, Tuple2<String,Integer>>
//        // ->左面：输入类型，代表每个单词
//        // ->右面：返回类型，代表每个单词和数值的组合
//        JavaRDD<Tuple2<String, Integer>> mapRdd = flatMapRdd.map(
//                f -> new Tuple2<String, Integer>(f, 1));
//
//        // Array((aa,1), (bb,1), (aa,1))  -->  Map(aa->List((aa,1),(aa,1))), bb->List((bb,1)))
//        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupByRdd = mapRdd.groupBy(f ->f._1);
//        // Map(aa->List((aa,1),(aa,1))), bb->List((bb,1))) --> Map(aa->2, bb->1)
//        JavaPairRDD<String, Integer> mapValuesRdd = groupByRdd.mapValues(f ->{
//                int sum = 0;
//                for (Tuple2<String, Integer> t : f) {
//                    sum += t._2;
//                }
//                return sum;
//        });
//
//        // 把executor端的转换结果拉取到driver端
//        List<Tuple2<String, Integer>> results = mapValuesRdd.collect();
//        for(Tuple2<String, Integer> t:results){
//            System.out.println(t);
//        }
//
//    }
//}
