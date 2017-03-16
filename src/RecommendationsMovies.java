import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class RecommendationsMovies {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws InterruptedException {
		String movieFile = "./testData/movies.dat";
		String ratingFile = "./testData/ratings.dat";

		SparkConf conf = new SparkConf().setAppName("recommendations");
		conf.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// format: (movieId, movieName)
		//TODO: load the movie data and convert into the (movieId, movieName) format.
		JavaRDD<Tuple2<Integer, String>> movieRDD = null;

		// format: (timestamp % 10, rating)
		//TODO: load the rating data and convert into the  (timestamp % 10, rating) format.
		JavaRDD<Tuple2<Long, Rating>> ratingRDD = null;

		printDataCount(ratingRDD);

		JavaPairRDD<Integer, String> moviePairRDD = movieRDD.mapToPair(p -> {
			return new Tuple2<Integer, String>(p._1, p._2);
		});

		
		Map<Integer, String> movieList = moviePairRDD.collectAsMap();

		// Get Top 50 voted movies.
		List<Integer> movieIdList = getTopVotedMovie(ratingRDD, 50);

		// Sample the data
		Map<Integer, String> movies = movieIdList
				.stream()
				.filter(p -> Math.random() < 0.2)
				.collect(
						Collectors.toMap(p -> p,
								p -> movieList.get(p)));

		System.out.println(movies);

		JavaRDD<Rating> myRating = elicitateRatings(movies, sc);

		// Training the model, dividing the data into training, validation and
		// testing.
		int numPartitions = 20;
		JavaRDD<Rating> trainingRDD = ratingRDD.filter(p -> p._1 < 6)
				.map(p -> p._2).union(myRating).repartition(numPartitions)
				.persist(StorageLevel.DISK_ONLY());

		JavaRDD<Rating> validationRDD = ratingRDD
				.filter(p -> p._1 >= 6 && p._1 < 8).map(p -> p._2)
				.union(myRating).repartition(numPartitions)
				.persist(StorageLevel.DISK_ONLY());

		JavaRDD<Rating> testRDD = ratingRDD.filter(p -> p._1 >= 8)
				.map(p -> p._2).union(myRating).repartition(numPartitions)
				.persist(StorageLevel.DISK_ONLY());

		System.out.println("Training: " + trainingRDD.count()
				+ ", validation: " + validationRDD.count() + ", test: "
				+ testRDD.count());

		int rank = 12;
		int numIterations = 10;

		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingRDD),
				rank, numIterations, 0.1);

		System.out.println(computeRmse(model, validationRDD));

		// predict my movie
		// 
		List<Integer> myRatedMovieIds = myRating.map(p -> p.product())
				.collect();

		JavaRDD<Integer> candidates = sc.parallelize(movieList.keySet().stream()
				.filter(p -> !myRatedMovieIds.contains(p))
				.collect(Collectors.toList()));

		JavaPairRDD<Integer, Integer> condidatesPair = candidates
				.mapToPair(p -> {
					return new Tuple2<Integer, Integer>(0, p);
				});

		ArrayList<Rating> recommendations = getRecommendations(model, condidatesPair);

		Collections.sort(recommendations, (p, q) -> {
			if (p.rating() > q.rating()) {
				return -1;
			} else {
				return 1;
			}
		});

		recommendations.subList(0, 50).forEach(
				p -> System.out.println(movieList.get(Integer.valueOf(
						p.product()))
						+ ": " + p.rating()));

		sc.close();
	}

	public static ArrayList<Rating> getRecommendations(
			final MatrixFactorizationModel model,
			JavaPairRDD<Integer, Integer> condidatesPair) {
		List<Rating> recommendations = null;
		
		// TODO: Your code here
		

		return new ArrayList<>(recommendations);
	}

	public static List<Integer> getTopVotedMovie(
			final JavaRDD<Tuple2<Long, Rating>> ratingRDD, int num) {
		List<Integer> movieIdList = null;
		
		// TODO: Your code here

		return movieIdList;
	}

	public static void printDataCount(JavaRDD<Tuple2<Long, Rating>> ratingRDD) {
		Long userCount = null, ratingCount = null, movieCount = null;
		
		// TODO: Your code here

		System.out.println("Got " + ratingCount + " ratings from " + userCount
				+ " users on " + movieCount + " movies.");
	}

	public static double computeRmse(MatrixFactorizationModel model,
			JavaRDD<Rating> ratings) {
		JavaRDD<Tuple2<Object, Object>> userMovies = ratings.map(p -> {
			return new Tuple2<Object, Object>(p.user(), p.product());
		});

		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD
				.fromJavaRDD(model
						.predict(JavaRDD.toRDD(userMovies))
						.toJavaRDD()
						.map(p -> {
							return new Tuple2<Tuple2<Integer, Integer>, Double>(
									new Tuple2<Integer, Integer>(p.user(), p
											.product()), p.rating());

						}));

		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD
				.fromJavaRDD(
						ratings.map(p -> {
							return new Tuple2<Tuple2<Integer, Integer>, Double>(
									new Tuple2<Integer, Integer>(p.user(), p
											.product()), p.rating());
						})).join(predictions);

		double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.values().map(p -> {
			Double err = p._1() - p._2();
			return (Object) (err * err);
		}).rdd()).mean();

		return Math.sqrt(MSE);
	}

	// Elicitate ratings from command-line.
	public static JavaRDD<Rating> elicitateRatings(
			Map<Integer, String> movieList, JavaSparkContext sc) {
		List<Rating> ratingList = new ArrayList<>();

		movieList
				.entrySet()
				.stream()
				.forEach(p -> {
					System.out.println(p.getValue());
					Scanner scanner = new Scanner(System.in);
					// The score is what we input from command line.
						Integer score = Integer.valueOf(scanner.nextLine());
						// the userID for ourselves is "0"
						ratingList.add(new Rating(Integer.valueOf("0"), p
								.getKey(), score));
					});

		return sc.parallelize(ratingList);
	}

}