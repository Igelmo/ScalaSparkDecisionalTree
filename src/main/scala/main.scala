import data.sqlQueries
import domain.MushroomMap.generateRandomMushroomToTest
import domain.{MushroomMap, generateDecisionalTree}

import scala.io.StdIn
object main {

  def main(args: Array[String]) = {

    val ingestSQLQuery = new sqlQueries
    val decisionalTreeInstance = new generateDecisionalTree
    val mushroomMap = MushroomMap

    ingestSQLQuery.showMushroomsDataFrame
    ingestSQLQuery.showDifferentCapColors

    println("Do you want to predict a random mushroom or a specific one? \n" +
      "random: 1, specified: 2")
    val option = StdIn.readLine()
    var mushroomToEvaluate: Array[Int] = Array.emptyIntArray
    option match {
      case "1" => {
        println("Generating random mushroom...")
        mushroomToEvaluate = generateRandomMushroomToTest
        println()
        println(mushroomToEvaluate.mkString("Array(", ", ", ")"))
      }
      case "2" => {
        println("Please, enter your mushroom characteristics to predict if it's edible or not:")
        val newMushroom = mushroomMap.introduceMushroom()
        println("You have introduced the next mushroom:")
        println(newMushroom.mkString("Array(", ", ", ")"))
        mushroomToEvaluate = mushroomMap.convertMushroomToMappedIntMushroom(newMushroom)
      }
      case _ => println("that's not an option")
    }

    decisionalTreeInstance.generateDecisionalTree(mushroomToEvaluate)

  }


}
