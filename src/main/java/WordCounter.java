import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCounter {
    public static void main(String[] args) {
        System.out.println("Starting Spark WordCounter...");
        if (args.length < 1) {
            System.err.println("For usage need to enter filePath.");
            System.exit(1);
        }

        SparkSession sparkSession = SparkSession.builder().config("spark.master", "local").appName("WordCount").getOrCreate();

        JavaRDD<String> linesOfText = sparkSession.read().textFile(args[0]).javaRDD();

        JavaRDD<String> wordInEachLine = linesOfText.flatMap(s -> Arrays.asList(s.replaceAll("[,.]","").split(" ")).iterator());

        JavaPairRDD<String, Integer> allTheOnes = wordInEachLine.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> finalCounts = allTheOnes.reduceByKey(Integer::sum).sortByKey();

        List<Tuple2<String, Integer>> output = finalCounts.collect();

        finalCounts.saveAsTextFile("Data/output");

        for (Tuple2<?, ?> tuple : output)
            System.out.println(tuple._1() + " " + tuple._2());

        sparkSession.stop();
        System.out.println("Finished");


    }
}
