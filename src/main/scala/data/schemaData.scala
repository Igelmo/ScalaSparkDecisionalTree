package data

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object schemaData {

  private val schema: StructType = StructType(
      Array(
        StructField("edible", StringType),
        StructField("cap_shape", StringType),
        StructField("cap_surface", StringType),
        StructField("cap_color", StringType),
        StructField("bruises", StringType),
        StructField("odor", StringType),
        StructField("gill_attachment", StringType),
        StructField("gill_spacing", StringType),
        StructField("gill_size", StringType),
        StructField("gill_color", StringType),
        StructField("stalk_shape", StringType),
        StructField("stalk_root", StringType),
        StructField("stalk_surface_above_ring", StringType),
        StructField("stalk_surface_below_ring", StringType),
        StructField("stalk_color_above_ring", StringType),
        StructField("stalk_color_below_ring", StringType),
        StructField("veil_type", StringType),
        StructField("veil_color", StringType),
        StructField("ring_number", StringType),
        StructField("ring_type", StringType),
        StructField("spore_print_color", StringType),
        StructField("population", StringType),
        StructField("habitat", StringType)
      )
    )

  def getSchema: StructType = schema

}