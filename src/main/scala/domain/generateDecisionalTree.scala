package domain

import data.ingestMushroomData
import domain.mushroomMap.createLabeledPoints
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Random

class generateDecisionalTree {

  val ingestInstance = new ingestMushroomData

  def mapCategoricalFeaturesInfo: Map[Int, Int] = {
    Map(0 -> 6,
      1 -> 6,
      2 -> 10,
      3 -> 2,
      4 -> 9,
      5 -> 4,
      6 -> 3,
      7 -> 2,
      8 -> 12,
      9 -> 2,
      10 -> 7,
      11 -> 4,
      12 -> 4,
      13 -> 9,
      14 -> 9,
      15 -> 2,
      16 -> 4,
      17 -> 3,
      18 -> 8,
      19 -> 9,
      20 -> 6,
      21 -> 7)
  }

  def generateRandomMushroomToTest: Array[Int] = {
    Array(
      Random.nextInt(5),
      Random.nextInt(5),
      Random.nextInt(9),
      Random.nextInt(1),
      Random.nextInt(8),
      Random.nextInt(3),
      Random.nextInt(2),
      Random.nextInt(1),
      Random.nextInt(11),
      Random.nextInt(1),
      Random.nextInt(6),
      Random.nextInt(3),
      Random.nextInt(3),
      Random.nextInt(8),
      Random.nextInt(8),
      Random.nextInt(1),
      Random.nextInt(3),
      Random.nextInt(2),
      Random.nextInt(7),
      Random.nextInt(8),
      Random.nextInt(5),
      Random.nextInt(6)
    )
  }

  val dataFrame: DataFrame = ingestInstance.getMushroomDataFrame

  val rddLabeledPoints: RDD[LabeledPoint] = dataFrame.rdd.map(createLabeledPoints)

  def generateDecisionalTree {
    val categoricalFeaturesInfo: Map[Int, Int] = mapCategoricalFeaturesInfo
    val mushroomToEvaluate = generateRandomMushroomToTest

    val decisionalTree = DecisionTree.trainClassifier (rddLabeledPoints, 2, categoricalFeaturesInfo, "gini", 5, 32)

    println (mushroomToEvaluate.mkString ("Array(", ", ", ")") )

    val testMushroom: Vector = Vectors.dense (mushroomToEvaluate.map (_.toDouble) )

    val predictions = decisionalTree.predict (testMushroom)

    println (decisionalTree.toDebugString)

    println (predictions)
  }
}
