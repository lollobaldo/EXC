package imdb

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]]) {
  def getGenres(): List[String] = genres.getOrElse(List[String]())
}
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  val conf: SparkConf = new SparkConf().setAppName("imdb-spark").setMaster("local[1]")
  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics)

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings)

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew)

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics)

  //  Calculate the average, minimum, and maximum runtime duration for all titles per movie genre.
  //    Note that a title can have more than one genre, thus it should be considered for all of them. The results
  //    should be kept in minutes and titles with 0 runtime duration are valid and should be accounted for in your
  //    solution.
  def task1(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {
    rdd
      .filter(x => x.runtimeMinutes.isDefined && x.genres.isDefined)
      .flatMap(x => x.genres.get.map((_, x.runtimeMinutes.get)))
      .groupBy(_._1)
      .map{case (genre, rs) => {
        val (sum, tot, mi, ma) = rs.foldLeft((0, 0, Int.MaxValue, 0))((t: (Int, Int, Int, Int), r: (_, Int)) =>
          (t._1 + r._2, t._2 + 1, r._2.min(t._3), r._2.max(t._4)))
        ((sum.toFloat / tot), mi, ma, genre)
      }}
  }

  //  Return the titles of the movies which were released between 1990 and 2018 (inclusive), have an average
  //  rating of 7.5 or more, and have received 500000 votes or more.
  //    For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’.
  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    val topRatings = l2
      .filter(x => x.averageRating >= 7.5 && x.numVotes >= 500000)
      .map(x => (x.tconst, 0))

    l1.filter(x =>
      x.startYear.isDefined && x.primaryTitle.isDefined && x.titleType.isDefined
        && x.startYear.get >= 1990 && x.startYear.get <= 2018 && x.titleType.get == "movie")
      .map(x => (x.tconst, x.primaryTitle.get))
      .join(topRatings)
      .map(x => x._2._1)
  }

  //  Return the top rated movie of each genre for each decade between 1900 and 1999.
  //  For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’. For
  //  calculating the top rated movies use the averageRating field and for the release year use the startYear
  //    field.
  //      The output should be sorted by decade and then by genre. For the movies with the same rating and of
  //  the same decade, print only the one with the title that comes first alphabetically. Each decade should be
  //    represented with a single digit, starting with 0 corresponding to 1900-1909.
  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    val ratingMap = l2.map(x => (x.tconst, x.averageRating));
    l1
      .filter(x => x.startYear.isDefined && x.genres.isDefined && x.startYear.get >= 1900 && x.startYear.get <= 1999
        && x.primaryTitle.isDefined && x.titleType.isDefined && x.titleType.get == "movie")
      .flatMap(x => x.genres.get.map((g => (x.tconst, (x.startYear.get, g, x.primaryTitle.get)))))
      .join(ratingMap)
      .map(x => (((x._2._1._1 / 10) % 10, x._2._1._2), (x._2._2, x._2._1._3)))
      .reduceByKey((x, y) => List(x,y).minBy(y => (-y._1, y._2)))
      .map{ case (key, value) => (key._1, key._2, value._2) }
      .sortBy(x => x)
  }

  //  In this task we are interested in all the crew names (primaryName) for whom there are at least two knownfor
  //    films released since the year 2010 up to and including the current year (2021).
  //  You need to return the crew name and the number of such films.
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {
    return sc.emptyRDD[(String, Int)]
    val newMovies = l1
      .filter(x => x.startYear.isDefined && x.startYear.get >= 2010 && x.startYear.get <= 2021)
      .map(x => (x.tconst, 0))
    l3
      .filter(x => x.primaryName.isDefined && x.knownForTitles.isDefined)
      .flatMap(x => x.knownForTitles.get.map(y => (y, x.primaryName.get)))
      .join(newMovies)
      .map{ case (key, value) => (value._1, key) }
      .aggregateByKey(0)(((u, _) => u+1), _+_)
//      .map(x => (x.primaryName.get, x.knownForTitles.get.count(y => newMovies.contains(y))))
  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val crews = timed("Task 4", task4(titleBasicsRDD, titleCrewRDD, nameBasicsRDD).collect().toList)
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
