package com.phareco.spark

import org.apache.spark._
import org.apache.log4j._

object PopularMovie {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "PopularMovies")
    val lines = sc.textFile("data/ml-100k/u.data")
    // File Format: (UserID, MovieID, Rating, Timestamp)
    val viewed = lines.map(x => (x.split("\t")(1).toInt, 1))
    val popularMoviesList = viewed.reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey()
    popularMoviesList.collect().foreach(println)
  }
}