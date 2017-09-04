import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkAssignment extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Tutorial")

  val sparkContext = new SparkContext(sparkConf)

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  //--------------------------- Question 1. Creating dataframe--------------------------------------------------

  val footballDF: DataFrame = sparkSession.read.option("header", "true").option("inferSchema","true").csv("src/main/resources/D1.csv")//.withColumn("count", lit(1))

  footballDF.createOrReplaceTempView("FootballMatches")

  //--------------------------- Question 2. Total number of match played by each team as HOMETEAM.--------------------------------------------------

  sparkSession.sql("SELECT HomeTeam, count(HomeTeam) as home_matches_count FROM FootballMatches GROUP BY HomeTeam").createOrReplaceTempView("FootballHomeMatches")

  sparkSession.sql("SELECT DISTINCT AwayTeam, 0 as away_team_matches FROM FootballMatches WHERE AwayTeam NOT IN (SELECT HomeTeam FROM FootballMatches)").createOrReplaceTempView("FootballAwayMatchesWithZeros")

  sparkSession.sql("SELECT * FROM FootballHomeMatches UNION SELECT * FROM FootballAwayMatchesWithZeros").show(false)

  //--------------------------- Question 3. Top 10 team with highest wining percentage.--------------------------------------------------

  sparkSession.sql("SELECT AwayTeam, count(AwayTeam) as away_matches_count FROM FootballMatches GROUP BY AwayTeam").createOrReplaceTempView("FootballAwayMatches")

  sparkSession.sql("SELECT HomeTeam, count(HomeTeam) as home_wins_count FROM FootballMatches WHERE FTR = 'H' GROUP BY HomeTeam").createOrReplaceTempView("FootballHomeWins")

  sparkSession.sql("SELECT AwayTeam, count(AwayTeam) as away_wins_count FROM FootballMatches WHERE FTR = 'A' GROUP BY AwayTeam").createOrReplaceTempView("FootballAwayWins")

  sparkSession.sql("SELECT FootballHomeWins.HomeTeam as team, (FootballHomeWins.home_wins_count + FootballAwayWins.away_wins_count) as totalwins FROM FootballHomeWins INNER JOIN FootballAwayWins ON FootballHomeWins.HomeTeam = FootballAwayWins.AwayTeam").createOrReplaceTempView("TotalWins")

  sparkSession.sql("SELECT FootballHomeMatches.HomeTeam as team, (FootballHomeMatches.home_matches_count + FootballAwayMatches.away_matches_count) as totalplayed FROM FootballHomeMatches INNER JOIN FootballAwayMatches ON FootballHomeMatches.HomeTeam = FootballAwayMatches.AwayTeam").createOrReplaceTempView("TotalPlayed")

  sparkSession.sql("SELECT TotalWins.team, (TotalWins.totalwins / TotalPlayed.totalplayed)*100 as percentage FROM TotalWins INNER JOIN TotalPlayed ON TotalWins.team = Totalplayed.team ORDER BY percentage DESC LIMIT 10").show(false)

  import sparkSession.implicits._

  //--------------------------- Dataset Question 4.--------------------------------------------------

  val footballDS: Dataset[FootballMatches] = footballDF.map[FootballMatches]((row: Row) =>
    FootballMatches(row.getString(2),row.getString(3), row.getInt(4), row.getInt(5), row.getString(6)))

  footballDS.show(false)

  //---------------------------Question 5. Total number of match played by each team.--------------------------------------------------

  footballDS.select($"HomeTeam").union(footballDS.select($"AwayTeam")).groupBy($"HomeTeam").count().show(false)

  //---------------------------Question 6. Top Ten team with highest wins.--------------------------------------------------

  footballDS.filter(row => row.FTR == "H").groupBy($"HomeTeam").count()
    .union(footballDS.filter(row => row.FTR == "A").groupBy($"AwayTeam").count()).groupBy($"HomeTeam")
    .sum("count").sort(desc("sum(count)")).limit(10).show(false)
  
}
