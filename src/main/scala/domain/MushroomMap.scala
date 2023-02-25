package domain

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

import scala.io.StdIn
import scala.util.Random

/*
1. cap-shape: bell=b,conical=c,convex=x,flat=f, knobbed=k,sunken=s
2. cap-surface: fibrous=f,grooves=g,scaly=y,smooth=s
3. cap-color: brown=n,buff=b,cinnamon=c,gray=g,green=r, pink=p,purple=u,red=e,white=w,yellow=y
4. bruises?: bruises=t,no=f
5. odor: almond=a,anise=l,creosote=c,fishy=y,foul=f, musty=m,none=n,pungent=p,spicy=s
6. gill-attachment: attached=a,descending=d,free=f,notched=n
7. gill-spacing: close=c,crowded=w,distant=d
8. gill-size: broad=b,narrow=n
9. gill-color: black=k,brown=n,buff=b,chocolate=h,gray=g, green=r,orange=o,pink=p,purple=u,red=e, white=w,yellow=y
10. stalk-shape: enlarging=e,tapering=t
11. stalk-root: bulbous=b,club=c,cup=u,equal=e, rhizomorphs=z,rooted=r,missing=?
12. stalk-surface-above-ring: fibrous=f,scaly=y,silky=k,smooth=s
13. stalk-surface-below-ring: fibrous=f,scaly=y,silky=k,smooth=s
14. stalk-color-above-ring: brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y
15. stalk-color-below-ring: brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y
16. veil-type: partial=p,universal=u
17. veil-color: brown=n,orange=o,white=w,yellow=y
18. ring-number: none=n,one=o,two=t
19. ring-type: cobwebby=c,evanescent=e,flaring=f,large=l, none=n,pendant=p,sheathing=s,zone=z
20. spore-print-color: black=k,brown=n,buff=b,chocolate=h,green=r, orange=o,purple=u,white=w,yellow=y
21. population: abundant=a,clustered=c,numerous=n, scattered=s,several=v,solitary=y
22. habitat: grasses=g,leaves=l,meadows=m,paths=p, urban=u,waste=w,woods=d
*/

object MushroomMap {

  def createLabeledPoints(row: Row): LabeledPoint = {
    val edible = edibleToInt(row.getAs("edible"))
    val cap_shape = capShapeToInt(row.getAs("cap_shape"))
    val cap_surface = capSurfaceToInt(row.getAs("cap_surface"))
    val cap_color = capColorToInt(row.getAs("cap_color"))
    val bruises = bruisesToInt(row.getAs("bruises"))
    val odor = odorToInt(row.getAs("odor"))
    val gill_attachment = gillAttachmentToIntToInt(row.getAs("gill_attachment"))
    val gill_spacing = gillSpacingToInt(row.getAs("gill_spacing"))
    val gill_size = gillSizeToInt(row.getAs("gill_size"))
    val gill_color = gillColorToInt(row.getAs("gill_color"))
    val stalk_shape = stalkShapeToInt(row.getAs("stalk_shape"))
    val stalk_root = stalkRootToInt(row.getAs("stalk_root"))
    val stalk_surface_above_ring = stalkSurfaceAboveRingToInt(row.getAs("stalk_surface_above_ring"))
    val stalk_surface_below_ring = stalkSurfaceBelowRingToInt(row.getAs("stalk_surface_below_ring"))
    val stalk_color_above_ring = stalkColorAboveRingToInt(row.getAs("stalk_color_above_ring"))
    val stalk_color_below_ring = stalkColorBelowRingToInt(row.getAs("stalk_color_below_ring"))
    val veil_type = veilTypeToInt(row.getAs("veil_type"))
    val veil_color = veilColorToInt(row.getAs("veil_color"))
    val ring_number = ringNumberToInt(row.getAs("ring_number"))
    val ring_type = ringTypeToInt(row.getAs("ring_type"))
    val spore_print_color = sporePrintColorToInt(row.getAs("spore_print_color"))
    val population = populationToInt(row.getAs("population"))
    val habitat = habitatToInt(row.getAs("habitat"))

    val attributes : Vector = Vectors.dense(Array(cap_shape, cap_surface, cap_color, bruises, odor, gill_attachment, gill_spacing, gill_size,
      gill_color, stalk_shape, stalk_root, stalk_surface_above_ring, stalk_surface_below_ring, stalk_color_above_ring,
      stalk_color_below_ring, veil_type, veil_color, ring_number, ring_type, spore_print_color, population, habitat).map(_.toDouble))

    LabeledPoint(edible, attributes)
  }



  //edible column (poisonous=p,edible=e)
  def edibleToInt(value: String) : Int = {
    value match {
      case "e" => 0
      case _ => 1
    }
  }

  //cap_shape column (bell=b,conical=c,convex=x,flat=f, knobbed=k,sunken=s)
  def capShapeToInt(value: String): Int = {
    value match {
      case "b" => 0
      case "c" => 1
      case "x" => 2
      case "f" => 3
      case "k" => 4
      case "s" => 5
    }
  }

  //cap_surface column (fibrous=f,grooves=g,scaly=y,smooth=s)
  def capSurfaceToInt(value: String): Int = {
    value match {
      case "f" => 0
      case "g" => 1
      case "y" => 2
      case "s" => 3
      case "k" => 4
      case "s" => 5
    }
  }

  //cap_color column (brown=n,buff=b,cinnamon=c,gray=g,green=r, pink=p,purple=u,red=e,white=w,yellow=y)
  def capColorToInt(value: String): Int = {
    value match {
      case "n" => 0
      case "b" => 1
      case "c" => 2
      case "g" => 3
      case "r" => 4
      case "p" => 5
      case "u" => 6
      case "e" => 7
      case "w" => 8
      case "y" => 9
    }
  }


  //bruises column (bruises=t,no=f)
  def bruisesToInt(value: String): Int = {
    value match {
      case "t" => 0
      case "f" => 1
    }
  }

  //odor column almond=a,anise=l,creosote=c,fishy=y,foul=f, musty=m,none=n,pungent=p,spicy=s
  def odorToInt(value: String): Int = {
    value match {
      case "a" => 0
      case "l" => 1
      case "c" => 2
      case "y" => 3
      case "f" => 4
      case "m" => 5
      case "n" => 6
      case "p" => 7
      case "s" => 8
    }
  }

  //gill_attachment column attached=a,descending=d,free=f,notched=n
  def gillAttachmentToIntToInt(value: String): Int = {
    value match {
      case "a" => 0
      case "d" => 1
      case "f" => 2
      case "n" => 3
    }
  }

  //gill_spacing column close=c,crowded=w,distant=d
  def gillSpacingToInt(value: String): Int = {
    value match {
      case "c" => 0
      case "w" => 1
      case "d" => 2
    }
  }


  //gill_size column broad=b,narrow=n
  def gillSizeToInt(value: String): Int = {
    value match {
      case "b" => 0
      case "n" => 1
    }
  }

  //gill_color column black=k,brown=n,buff=b,chocolate=h,gray=g, green=r,orange=o,pink=p,purple=u,red=e, white=w,yellow=y
  def gillColorToInt(value: String): Int = {
    value match {
      case "k" => 0
      case "n" => 1
      case "b" => 2
      case "h" => 3
      case "g" => 4
      case "r" => 5
      case "o" => 6
      case "p" => 7
      case "u" => 8
      case "e" => 9
      case "w" => 10
      case "y" => 11
    }
  }

  //stalk_shape column enlarging=e,tapering=t
  def stalkShapeToInt(value: String): Int = {
    value match {
      case "e" => 0
      case "t" => 1
    }
  }

  //stalk_root column bulbous=b,club=c,cup=u,equal=e, rhizomorphs=z,rooted=r,missing=?
  def stalkRootToInt(value: String): Int = {
    value match {
      case "b" => 0
      case "c" => 1
      case "u" => 2
      case "e" => 3
      case "z" => 4
      case "r" => 5
      case "?" => 6
    }
  }

  //stalk_surface_above_ring column fibrous=f,scaly=y,silky=k,smooth=s
  def stalkSurfaceAboveRingToInt(value: String): Int = {
    value match {
      case "f" => 0
      case "y" => 1
      case "k" => 2
      case "s" => 3
    }
  }

  //stalk_surface_below_ring column fibrous=f,scaly=y,silky=k,smooth=s
  def stalkSurfaceBelowRingToInt(value: String): Int = {
    value match {
      case "f" => 0
      case "y" => 1
      case "k" => 2
      case "s" => 3
    }
  }


  //stalk_color_above_ring: brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y
  def stalkColorAboveRingToInt(value: String): Int = {
    value match {
      case "n" => 0
      case "b" => 1
      case "c" => 2
      case "g" => 3
      case "o" => 4
      case "p" => 5
      case "e" => 6
      case "w" => 7
      case "y" => 8
    }
  }

  //stalk_color_below_ring: brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y
  def stalkColorBelowRingToInt(value: String): Int = {
    value match {
      case "n" => 0
      case "b" => 1
      case "c" => 2
      case "g" => 3
      case "o" => 4
      case "p" => 5
      case "e" => 6
      case "w" => 7
      case "y" => 8
    }
  }

  //veil_type column partial=p,universal=u
  def veilTypeToInt(value: String): Int = {
    value match {
      case "p" => 0
      case "u" => 1
    }
  }

  //veil_color column brown=n,orange=o,white=w,yellow=y
  def veilColorToInt(value: String): Int = {
    value match {
      case "n" => 0
      case "o" => 1
      case "w" => 2
      case "y" => 3
    }
  }

  //ring_number column none=n,one=o,two=t
  def ringNumberToInt(value: String): Int = {
    value match {
      case "n" => 0
      case "o" => 1
      case "t" => 2
    }
  }

  //ring_type column cobwebby=c,evanescent=e,flaring=f,large=l, none=n,pendant=p,sheathing=s,zone=z
  def ringTypeToInt(value: String): Int = {
    value match {
      case "c" => 0
      case "e" => 1
      case "f" => 2
      case "l" => 3
      case "n" => 4
      case "p" => 5
      case "s" => 6
      case "z" => 7
    }
  }

  //spore_print_color column black=k,brown=n,buff=b,chocolate=h,green=r, orange=o,purple=u,white=w,yellow=y
  def sporePrintColorToInt(value: String): Int = {
    value match {
      case "k" => 0
      case "n" => 1
      case "b" => 2
      case "h" => 3
      case "r" => 4
      case "o" => 5
      case "u" => 6
      case "w" => 7
      case "y" => 8
    }
  }

  //population column abundant=a,clustered=c,numerous=n, scattered=s,several=v,solitary=y
  def populationToInt(value: String): Int = {
    value match {
      case "a" => 0
      case "c" => 1
      case "n" => 2
      case "s" => 3
      case "v" => 4
      case "y" => 5
    }
  }

  //habitat column grasses=g,leaves=l,meadows=m,paths=p, urban=u,waste=w,woods=d
  def habitatToInt(value: String): Int = {
    value match {
      case "g" => 0
      case "l" => 1
      case "m" => 2
      case "p" => 3
      case "u" => 4
      case "w" => 5
      case "d" => 6
    }
  }

  def convertMushroomToMappedIntMushroom(mushroom: Array[String]) : Array[Int] = {
    Array (
      capShapeToInt(mushroom(0)),
      capSurfaceToInt(mushroom(1)),
      capColorToInt(mushroom(2)),
      bruisesToInt(mushroom(3)),
      odorToInt(mushroom(4)),
      gillAttachmentToIntToInt(mushroom(5)),
      gillSpacingToInt(mushroom(6)),
      gillSizeToInt(mushroom(7)),
      gillColorToInt(mushroom(8)),
      stalkShapeToInt(mushroom(9)),
      stalkRootToInt(mushroom(10)),
      stalkSurfaceAboveRingToInt(mushroom(11)),
      stalkSurfaceBelowRingToInt(mushroom(12)),
      stalkColorAboveRingToInt(mushroom(13)),
      stalkColorBelowRingToInt(mushroom(14)),
      veilTypeToInt(mushroom(15)),
      veilColorToInt(mushroom(16)),
      ringNumberToInt(mushroom(17)),
      ringTypeToInt(mushroom(18)),
      sporePrintColorToInt(mushroom(19)),
      populationToInt(mushroom(20)),
      habitatToInt(mushroom(21))
    )
  }

  def introduceMushroom(): Array[String] = {

    println("Introduce cap-shape, options are:\n" +
      "bell=b,conical=c,convex=x,flat=f, knobbed=k,sunken=s")
    val capShape = StdIn.readLine()

    println("Introduce cap-surface, options are:\n" +
      "fibrous=f,grooves=g,scaly=y,smooth=s")
    val capSurface = StdIn.readLine()

    println("Introduce cap-color, options are:\n" +
      "brown=n,buff=b,cinnamon=c,gray=g,green=r, pink=p,purple=u,red=e,white=w,yellow=y")
    val capColor = StdIn.readLine()

    println("Introduce if it has bruises, options are:\n" +
      "bruises=t,no=f")
    val bruises = StdIn.readLine()

    println("Introduce odor, options are:\n" +
      "almond=a,anise=l,creosote=c,fishy=y,foul=f, musty=m,none=n,pungent=p,spicy=s")
    val odor = StdIn.readLine()

    println("Introduce gill attachment, options are:\n" +
      "attached=a,descending=d,free=f,notched=n")
    val gillAttachment = StdIn.readLine()

    println("Introduce gill spacing, options are:\n" +
      "close=c,crowded=w,distant=d")
    val gillSpacing = StdIn.readLine()

    println("Introduce gill size, options are:\n" +
      "broad=b,narrow=n")
    val gillSize = StdIn.readLine()

    println("Introduce gill color, options are:\n" +
      "black=k,brown=n,buff=b,chocolate=h,gray=g, green=r,orange=o,pink=p,purple=u,red=e, white=w,yellow=y")
    val gillColor = StdIn.readLine()

    println("Introduce stalk shape, options are:\n" +
      "enlarging=e,tapering=t")
    val stalkShape = StdIn.readLine()

    println("Introduce stalk root, options are:\n" +
      "bulbous=b,club=c,cup=u,equal=e, rhizomorphs=z,rooted=r,missing=?")
    val stalkRoot = StdIn.readLine()

    println("Introduce stalk surface above ring, options are:\n" +
      "fibrous=f,scaly=y,silky=k,smooth=s")
    val stalkSurfaceAboveRing = StdIn.readLine()

    println("Introduce stalk surface below ring, options are:\n" +
      "fibrous=f,scaly=y,silky=k,smooth=s")
    val stalkSurfaceBelowRing = StdIn.readLine()

    println("Introduce stalk color above ring, options are:\n" +
      "brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y")
    val stalkColorAboveRing = StdIn.readLine()

    println("Introduce stalk color below ring, options are:\n" +
      "brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y")
    val stalkColorBelowRing = StdIn.readLine()

    println("Introduce veil type, options are:\n" +
      "partial=p,universal=u")
    val veilType = StdIn.readLine()

    println("Introduce veil color, options are:\n" +
      "brown=n,orange=o,white=w,yellow=y")
    val veilColor = StdIn.readLine()

    println("Introduce veil number, options are:\n" +
      "none=n,one=o,two=t")
    val veilNumber = StdIn.readLine()

    println("Introduce ring type, options are:\n" +
      "cobwebby=c,evanescent=e,flaring=f,large=l, none=n,pendant=p,sheathing=s,zone=z")
    val ringType = StdIn.readLine()

    println("Introduce spore print color, options are:\n" +
      "black=k,brown=n,buff=b,chocolate=h,green=r, orange=o,purple=u,white=w,yellow=y")
    val sporePrintColor = StdIn.readLine()

    println("Introduce population, options are:\n" +
      "abundant=a,clustered=c,numerous=n, scattered=s,several=v,solitary=y")
    val population = StdIn.readLine()

    println("Introduce habitat, options are:\n" +
      "grasses=g,leaves=l,meadows=m,paths=p, urban=u,waste=w,woods=d")
    val habitat = StdIn.readLine()

    Array(
      capShape,
      capSurface,
      capColor,
      bruises,
      odor,
      gillAttachment,
      gillSpacing,
      gillSize,
      gillColor,
      stalkShape,
      stalkRoot,
      stalkSurfaceAboveRing,
      stalkSurfaceBelowRing,
      stalkColorAboveRing,
      stalkColorBelowRing,
      veilType,
      veilColor,
      veilNumber,
      ringType,
      sporePrintColor,
      population,
      habitat
    )
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

}
