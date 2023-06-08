package org.cheney.fake.generator

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}

object CreateMapData {
  def create(keys: Seq[Object], values: Seq[Object]): MapData = {
    ArrayBasedMapData.apply(keys.toArray[Object], values.toArray[Object])
  }
}
