import data.{sqlQueries}
import domain.generateDecisionalTree
object main {

  def main(args: Array[String]) = {

    val ingestSQLQuery = new sqlQueries
    val decisionalTreeInstance = new generateDecisionalTree

    ingestSQLQuery.showMushroomsDataFrame
    ingestSQLQuery.showDifferentCapColors

    decisionalTreeInstance.generateDecisionalTree

  }


}
