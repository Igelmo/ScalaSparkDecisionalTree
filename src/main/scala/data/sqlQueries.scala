package data

import org.apache.spark.sql.DataFrame

class sqlQueries {

  val ingestInstance = new ingestMushroomData

  val mushroomDataFrame: DataFrame = ingestInstance.getMushroomDataFrame
  private val mushrooms: DataFrame = initDataFrameForQueries

  private def initDataFrameForQueries: DataFrame ={
    val mushrooms: DataFrame = ingestInstance.transformDBToMeaningfullStrings(mushroomDataFrame)
    mushrooms.createOrReplaceTempView("Mushrooms")
    mushrooms
  }

  def showMushroomsDataFrame: Unit = mushrooms.show(50, false)

  def showDifferentCapColors: Unit = ingestInstance.spark.sql("SELECT DISTINCT cap_color FROM Mushrooms").show
}
