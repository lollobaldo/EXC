package imdb

import scala.collection.mutable

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                       originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                       runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  //  val source = scala.io.Source.fromFile("file.txt")
  //  val lines = try source.mkString finally source.close()
  val titleBasicsList: List[TitleBasics] = io.Source.fromFile(ImdbData.titleBasicsPath).getLines()
    .map(ImdbData.parseTitleBasics).toList

  val titleRatingsList: List[TitleRatings] = io.Source.fromFile(ImdbData.titleRatingsPath).getLines()
    .map(ImdbData.parseTitleRatings).toList

  val titleCrewList: List[TitleCrew] = io.Source.fromFile(ImdbData.titleCrewPath).getLines()
    .map(ImdbData.parseTitleCrew).toList

  val nameBasicsList: List[NameBasics] = io.Source.fromFile(ImdbData.nameBasicsPath).getLines()
    .map(ImdbData.parseNameBasics).toList

  //  Calculate the average, minimum, and maximum runtime duration for all titles per movie genre.
  //    Note that a title can have more than one genre, thus it should be considered for all of them. The results
  //    should be kept in minutes and titles with 0 runtime duration are valid and should be accounted for in your
  //    solution.
  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {
    //    list
    //      .filter(x => x.runtimeMinutes.isDefined && x.genres.isDefined)
    //      .flatMap(x => x.genres.get.map((_, x.runtimeMinutes.get)))
    //      .groupBy(_._1)
    //      .map{case (genre, rs) => {
    //        val (sum, tot, mi, ma) = rs.foldLeft((0, 0, Int.MaxValue, 0))((t: (Int, Int, Int, Int), r: (_, Int)) =>
    //          (t._1 + r._2, t._2 + 1, r._2.min(t._3), r._2.max(t._4)))
    //        ((sum / tot).toFloat, mi, ma, genre)
    //      }}
    //      .toList

    list.foldLeft(new mutable.AnyRefMap(100): mutable.AnyRefMap[String, (Int, Int, Int, Int)])({ (acc, obj) =>
      (obj.genres, obj.runtimeMinutes) match {
        case (Some(k), Some(r)) =>
          k.foldLeft(acc)((a, ky) => {
            val t = acc.getOrElse(ky, (0, 0, Int.MaxValue, 0))
            acc.update(ky, (t._1 + r, t._2 + 1, r.min(t._3), r.max(t._4)))
            acc
          })
        case _ => acc
      }
    })
      .map{case (genre, (sum, tot, mi, ma)) => {
        ((sum.toFloat / tot), mi, ma, genre)
      }}
      .toList
  }

  //  Return the titles of the movies which were released between 1990 and 2018 (inclusive), have an average
  //  rating of 7.5 or more, and have received 500000 votes or more.
  //    For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’.
  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    //    val topRatings = l2
    //      .filter(x => x.averageRating >= 7.5 && x.numVotes >= 500000)
    //      .map(_.tconst)
    //      .toSet
    //    l1.filter(x =>
    //      x.startYear.isDefined && x.primaryTitle.isDefined && x.titleType.isDefined
    //        && x.startYear.get >= 1990 && x.startYear.get <= 2018 && x.titleType.get == "movie"
    //        && topRatings.contains(x.tconst))
    //      .map(_.primaryTitle.get)

    val l = l2.length
    val topRatings = l2.foldLeft(new mutable.AnyRefMap(l*2): mutable.AnyRefMap[String, Float]){ (acc, obj) =>
      if (obj.averageRating >= 7.5 && obj.numVotes >= 500000) {
        acc.update(obj.tconst, obj.averageRating)
      }
      acc
    }
    l1.filter(x =>
      x.startYear.isDefined && x.primaryTitle.isDefined && x.titleType.isDefined
        && x.startYear.get >= 1990 && x.startYear.get <= 2018 && x.titleType.get == "movie"
        && topRatings.contains(x.tconst))
      .map(_.primaryTitle.get)
  }

  //  Return the top rated movie of each genre for each decade between 1900 and 1999.
  //  For the titles use the primaryTitle field and account only for entries whose titleType is ‘movie’. For
  //  calculating the top rated movies use the averageRating field and for the release year use the startYear
  //    field.
  //      The output should be sorted by decade and then by genre. For the movies with the same rating and of
  //  the same decade, print only the one with the title that comes first alphabetically. Each decade should be
  //    represented with a single digit, starting with 0 corresponding to 1900-1909.
  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = {
    //    val ratingMap = l2.map(x => (x.tconst, x.averageRating)).toMap;
    val l = l2.length
    val ratingMap = l2.foldLeft(new mutable.AnyRefMap(l*2): mutable.AnyRefMap[String, Float]){ (acc, obj) =>
      acc.update(obj.tconst, obj.averageRating)
      acc
    }
    val movies = l1
      .filter(x => x.startYear.isDefined && x.genres.isDefined && x.startYear.get <= 1999
        && x.primaryTitle.isDefined && x.titleType.isDefined && x.titleType.get == "movie" && ratingMap.contains(x.tconst))
      .flatMap(x => x.genres.get.map((g => (ratingMap(x.tconst), x.startYear.get, g, x.primaryTitle.get))))
      .groupBy(x => ((x._2/ 10) % 10, x._3))
      //      .mapValues(x => x.minBy(y => (-y._1, y._4))._4)
      .map{ case (key, value) => (key._1, key._2, value.minBy(y => (-y._1, y._4))._4) }
      .toList
      .sorted
    movies
  }

  //  In this task we are interested in all the crew names (primaryName) for whom there are at least two knownfor
  //    films released since the year 2010 up to and including the current year (2021).
  //  You need to return the crew name and the number of such films.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {
    val newMovies = l1
      .filter(x => x.startYear.isDefined && x.startYear.get >= 2010 && x.startYear.get <= 2021)
      .map(_.tconst)
      .toSet
    l3
      .filter(x => x.primaryName.isDefined && x.knownForTitles.isDefined
        && x.knownForTitles.get.filter(y => newMovies.contains(y)).length >= 2)
      .map(x => (x.primaryName.get, x.knownForTitles.get.filter(y => newMovies.contains(y)).length))
  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsList))
    val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
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
