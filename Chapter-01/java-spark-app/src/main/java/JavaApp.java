import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * A simple Spark app in Java
 */
public class JavaApp {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
        // we take the raw data in CSV format and convert it into a set of records of the form (user, product, price)
        JavaRDD<String[]> data = sc.textFile("data/UserPurchaseHistory.csv")
                .map((Function<String, String[]>) s -> s.split(","));

        // let's count the number of purchases
        long numPurchases = data.count();

        // let's count how many unique users made purchases
        long uniqueUsers = data.map((Function<String[], String>) strings -> strings[0]).distinct().count();

        // let's sum up our total revenue
        double totalRevenue = data.mapToDouble((DoubleFunction<String[]>) strings -> Double.parseDouble(strings[2])).sum();

        // let's find our most popular product
        // first we map the data to records of (product, 1) using a PairFunction
        // and the Tuple2 class.
        // then we call a reduceByKey operation with a Function2, which is essentially the sum function
        List<Tuple2<String, Integer>> pairs = data.mapToPair((PairFunction<String[], String, Integer>) strings -> new Tuple2(strings[1], 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2).collect();

        // finally we sort the result. Note we need to create a Comparator function,
        // that reverses the sort order.
        pairs.sort((o1, o2) -> -(o1._2() - o2._2()));
        String mostPopular = pairs.get(0)._1();
        int purchases = pairs.get(0)._2();

        // print everything out
        System.out.println("Total purchases: " + numPurchases);
        System.out.println("Unique users: " + uniqueUsers);
        System.out.println("Total revenue: " + totalRevenue);
        System.out.println(String.format("Most popular product: %s with %d purchases",
                mostPopular, purchases));

        sc.stop();

    }

}
