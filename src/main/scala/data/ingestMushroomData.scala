package data

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ingestMushroomData {


  val spark = SparkSession
    .builder()
    .appName("Agaricus Lepiota")
    .master("local")
    .getOrCreate()

  def readMushroomData : DataFrame = {
    spark.read.format("txt")
              .option("multiline", value = true)
              .option("quotes", ",")
              .schema(schemaData.getSchema)
              .csv("data/agaricus-lepiota.data")
  }

  val mushroomDataFrame: DataFrame = readMushroomData

  def getMushroomDataFrame: DataFrame = {
    mushroomDataFrame
  }

  def transformDBToMeaningfullStrings(dataFrame: DataFrame): DataFrame = {

    //edible column (poisonous=p,edible=e)
    dataFrame.withColumn("edible",
      when(col("edible") === "p", "Poisonous").
      otherwise("Edible")
    )

    //cap_shape column (bell=b,conical=c,convex=x,flat=f, knobbed=k,sunken=s)
    .withColumn("cap_shape",
      when(col("cap_shape") === "b", "Bell").
      when(col("cap_shape") === "c", "Conical").
      when(col("cap_shape") === "x", "Convex").
      when(col("cap_shape") === "f", "Flat").
      when(col("cap_shape") === "k", "Knobbed").
      when(col("cap_shape") === "s", "Sunken").
      otherwise("Unknown")
    )

    //cap_surface column (fibrous=f,grooves=g,scaly=y,smooth=s)
    .withColumn("cap_surface",
      when(col("cap_surface") === "f", "Fibrous").
      when(col("cap_surface") === "g", "Grooves").
      when(col("cap_surface") === "y", "Scaly").
      when(col("cap_surface") === "s", "Smooth").
      otherwise("Unknown")
    )

    //cap_color column (brown=n,buff=b,cinnamon=c,gray=g,green=r, pink=p,purple=u,red=e,white=w,yellow=y)
    .withColumn("cap_color",
      when(col("cap_color") === "n", "Brown").
      when(col("cap_color") === "b", "Buff").
      when(col("cap_color") === "c", "Cinnamon").
      when(col("cap_color") === "g", "Gray").
      when(col("cap_color") === "r", "Green").
      when(col("cap_color") === "p", "Pink").
      when(col("cap_color") === "u", "Purple").
      when(col("cap_color") === "e", "Red").
      when(col("cap_color") === "w", "White").
      when(col("cap_color") === "y", "Yellow").
      otherwise("Unknown")
    )

    //bruises column (bruises=t,no=f)
    .withColumn("bruises",
      when(col("bruises") === "t", "True").
      when(col("bruises") === "f", "False").
      otherwise("Unknown")
    )

    //odor column almond=a,anise=l,creosote=c,fishy=y,foul=f, musty=m,none=n,pungent=p,spicy=s
    .withColumn("odor",
      when(col("odor") === "a", "Almond").
      when(col("odor") === "l", "Anise").
      when(col("odor") === "c", "Creosote").
      when(col("odor") === "y", "Fishy").
      when(col("odor") === "f", "Foul").
      when(col("odor") === "m", "Musty").
      when(col("odor") === "n", "None").
      when(col("odor") === "p", "Pungent").
      when(col("odor") === "s", "Spicy").
      otherwise("Unknown")
    )

    //gill_attachment column attached=a,descending=d,free=f,notched=n
    .withColumn("gill_attachment",
      when(col("gill_attachment") === "a", "Attached").
      when(col("gill_attachment") === "d", "Descending").
      when(col("gill_attachment") === "f", "Free").
      when(col("gill_attachment") === "n", "Notched").
      otherwise("Unknown")
    )

    //gill_spacing column close=c,crowded=w,distant=d
    .withColumn("gill_spacing",
      when(col("gill_spacing") === "c", "Close").
      when(col("gill_spacing") === "w", "Crowded").
      when(col("gill_spacing") === "d", "Distant").
      otherwise("Unknown")
    )

    //gill_size column broad=b,narrow=n
    .withColumn("gill_size",
      when(col("gill_size") === "b", "Board").
      when(col("gill_size") === "n", "Narrow").
      otherwise("Unknown")
    )

    //gill_color column black=k,brown=n,buff=b,chocolate=h,gray=g, green=r,orange=o,pink=p,purple=u,red=e, white=w,yellow=y
    .withColumn("gill_color",
      when(col("gill_color") === "k", "Black").
      when(col("gill_color") === "n", "Brown").
      when(col("gill_color") === "b", "Buff").
      when(col("gill_color") === "h", "Chocolate").
      when(col("gill_color") === "g", "Gray").
      when(col("gill_color") === "r", "Green").
      when(col("gill_color") === "o", "Orange").
      when(col("gill_color") === "p", "Pink").
      when(col("gill_color") === "u", "Purple").
      when(col("gill_color") === "e", "Red").
      when(col("gill_color") === "w", "White").
      when(col("gill_color") === "y", "Yellow").
      otherwise("Unknown")
    )

    //stalk_shape column enlarging=e,tapering=t
    .withColumn("stalk_shape",
      when(col("stalk_shape") === "e", "Enlarging").
      when(col("stalk_shape") === "t", "Tapering").
      otherwise("Unknown")
    )

    //stalk_root column bulbous=b,club=c,cup=u,equal=e, rhizomorphs=z,rooted=r,missing=?
    .withColumn("stalk_root",
      when(col("stalk_root") === "b", "Bulbous").
      when(col("stalk_root") === "c", "Club").
      when(col("stalk_root") === "u", "Cup").
      when(col("stalk_root") === "e", "Equal").
      when(col("stalk_root") === "z", "Rhizomorphs").
      when(col("stalk_root") === "r", "Rooted").
      when(col("stalk_root") === "?", "Missing").
      otherwise("Unknown")
    )

    //stalk_surface_above_ring column fibrous=f,scaly=y,silky=k,smooth=s
    .withColumn("stalk_surface_above_ring",
      when(col("stalk_surface_above_ring") === "f", "Fibrous").
      when(col("stalk_surface_above_ring") === "y", "Scaly").
      when(col("stalk_surface_above_ring") === "k", "Silky").
      when(col("stalk_surface_above_ring") === "s", "Smooth").
      otherwise("Unknown")
    )

    //stalk_surface_below_ring column fibrous=f,scaly=y,silky=k,smooth=s
    .withColumn("stalk_surface_below_ring",
      when(col("stalk_surface_below_ring") === "f", "Fibrous").
      when(col("stalk_surface_below_ring") === "y", "Scaly").
      when(col("stalk_surface_below_ring") === "k", "Silky").
      when(col("stalk_surface_below_ring") === "s", "Smooth").
      otherwise("Unknown")
    )

    //stalk_color_above_ring: brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y
    .withColumn("stalk_color_above_ring",
      when(col("stalk_color_above_ring") === "n", "Brown").
      when(col("stalk_color_above_ring") === "b", "Buff").
      when(col("stalk_color_above_ring") === "c", "Cinnamon").
      when(col("stalk_color_above_ring") === "g", "Gray").
      when(col("stalk_color_above_ring") === "o", "Orange").
      when(col("stalk_color_above_ring") === "p", "Pink").
      when(col("stalk_color_above_ring") === "e", "Red").
      when(col("stalk_color_above_ring") === "w", "White").
      when(col("stalk_color_above_ring") === "y", "Yellow").
      otherwise("Unknown")
    )

    //stalk_color_below_ring: brown=n,buff=b,cinnamon=c,gray=g,orange=o, pink=p,red=e,white=w,yellow=y
    .withColumn("stalk_color_below_ring",
      when(col("stalk_color_below_ring") === "n", "Brown").
      when(col("stalk_color_below_ring") === "b", "Buff").
      when(col("stalk_color_below_ring") === "c", "Cinnamon").
      when(col("stalk_color_below_ring") === "g", "Gray").
      when(col("stalk_color_below_ring") === "o", "Orange").
      when(col("stalk_color_below_ring") === "p", "Pink").
      when(col("stalk_color_below_ring") === "e", "Red").
      when(col("stalk_color_below_ring") === "w", "White").
      when(col("stalk_color_below_ring") === "y", "Yellow").
      otherwise("Unknown")
    )

    //veil_type column partial=p,universal=u
    .withColumn("veil_type",
      when(col("veil_type") === "p", "Partial").
      when(col("veil_type") === "u", "Universal").
        otherwise("Unknown")
    )

    //veil_color column brown=n,orange=o,white=w,yellow=y
    .withColumn("veil_color",
      when(col("veil_color") === "n", "Brown").
      when(col("veil_color") === "o", "Orange").
      when(col("veil_color") === "w", "White").
      when(col("veil_color") === "y", "Yellow").
      otherwise("Unknown")
    )

    //ring_number column none=n,one=o,two=t
    .withColumn("ring_number",
      when(col("ring_number") === "n", "None").
      when(col("ring_number") === "o", "One").
      when(col("ring_number") === "t", "Two").
      otherwise("Unknown")
    )

    //ring_type column cobwebby=c,evanescent=e,flaring=f,large=l, none=n,pendant=p,sheathing=s,zone=z
    .withColumn("ring_type",
      when(col("ring_type") === "c", "Cobwebby").
      when(col("ring_type") === "e", "Evanescent").
      when(col("ring_type") === "f", "Flaring").
      when(col("ring_type") === "l", "Large").
      when(col("ring_type") === "n", "None").
      when(col("ring_type") === "p", "Pendant").
      when(col("ring_type") === "s", "Sheathing").
      when(col("ring_type") === "z", "Zone").
      otherwise("Unknown")
    )

    //spore_print_color column black=k,brown=n,buff=b,chocolate=h,green=r, orange=o,purple=u,white=w,yellow=y
    .withColumn("spore_print_color",
      when(col("spore_print_color") === "k", "Black").
      when(col("spore_print_color") === "n", "Brown").
      when(col("spore_print_color") === "b", "Buff").
      when(col("spore_print_color") === "h", "Chocolate").
      when(col("spore_print_color") === "r", "Green").
      when(col("spore_print_color") === "o", "Orange").
      when(col("spore_print_color") === "u", "Purple").
      when(col("spore_print_color") === "w", "White").
      when(col("spore_print_color") === "y", "Yellow").
      otherwise("Unknown")
    )

    //population column abundant=a,clustered=c,numerous=n, scattered=s,several=v,solitary=y
    .withColumn("population",
      when(col("population") === "a", "Abundant").
      when(col("population") === "c", "Clustered").
      when(col("population") === "n", "Numerous").
      when(col("population") === "s", "Scattered").
      when(col("population") === "v", "Several").
      when(col("population") === "y", "Solitary").
      otherwise("Unknown")
    )

    //habitat column grasses=g,leaves=l,meadows=m,paths=p, urban=u,waste=w,woods=d
    .withColumn("habitat",
      when(col("habitat") === "g", "Grasses").
      when(col("habitat") === "l", "Leaves").
      when(col("habitat") === "m", "Meadows").
      when(col("habitat") === "p", "Paths").
      when(col("habitat") === "u", "Urban").
      when(col("habitat") === "w", "Waste").
      when(col("habitat") === "d", "Woods").
      otherwise("Unknown")
    )

  }

/*  def convertToMushroom(row: Row): Mushroom = {
    Mushroom(row.toString())
  }*/


}