package com.phareco.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object Recommender {
  def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a map of MovieID -> MovieName
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("data/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    ((movieRating1._1, movieRating2._1), (movieRating1._2, movieRating2._2))
  }

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    return movieRating1._1 < movieRating2._1
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def cosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    val numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_xy += ratingX * ratingY
      sum_yy += ratingY * ratingY
      
    }
    
    val numerator: Double = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)
    
    var score: Double = 0.0
    if (denominator != 0) 
      score = numerator / denominator
      
    return (score, numPairs)
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Recommender")
    val movieNames = loadMovieNames()
    val data = sc.textFile("data/ml-100k/u.data")
    val ratings = data.map(l => l.split("\t"))
      .map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))
    val joinedRatings = ratings.join(ratings)
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
    val moviePairs = uniqueJoinedRatings.map(makePairs)
    val moviePairRatings = moviePairs.groupByKey()
    val moviePairSimilarities = moviePairRatings.mapValues(cosineSimilarity).cache()
  }

}