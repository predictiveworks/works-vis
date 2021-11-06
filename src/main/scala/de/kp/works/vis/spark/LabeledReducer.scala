package de.kp.works.vis.spark
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

trait LabeledReducerParams extends Params {

  final val featuresCol = new Param[String](this, "featuresCol",
    "Name of features column", (_:String) => true)

  def setFeaturesCol(value:String): this.type = set(featuresCol, value)
  setDefault(featuresCol -> "features")

  final val labelCol = new Param[String](this, "labelCol",
    "Name of label column", (_:String) => true)

  def setLabelCol(value:String): this.type = set(labelCol, value)
  setDefault(labelCol -> "label")

  def getFeaturesCol:String = $(featuresCol)

  def getLabelCol:String = $(labelCol)

}

/**
 * [DimReducer] leverages PCA to reduce the dimensions
 * of feature vectors to 2 for enabling 2D visualization
 * of (vectorized) machine learning datasets.
 *
 * The result is a dataframe of columns label, x, y which
 * can be saved as as parquet file to enable visualization
 * by one of the (datashader) visualizers.
 */
class LabeledReducer(override val uid: String) extends Transformer with LabeledReducerParams {

  def this() = this(Identifiable.randomUID("dimReducer"))

  override def copy(extra: ParamMap): LabeledReducer = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    /*
     * Restrict to columns that are required for visualization
     */
    val prepareset = dataset.select($(featuresCol), $(labelCol))
    /*
     * Train PCA model and apply projection to prepared
     * dataset
     */
    val pca = new PCA()
      .setInputCol($(featuresCol))
      .setOutputCol("projected")
      .setK(2)
      .fit(prepareset)

    val projected = pca.transform(prepareset).select("projected", $(labelCol))

    val decompose_udf = udf((projected:DenseVector) => {
      val point = projected.toArray
      Seq(point(0), point(1))
    })

    projected
      .withColumn("point", decompose_udf(col("projected")))
      .withColumn("x", col("point").getItem(0))
      .withColumn("y", col("point").getItem(1))
      .drop("point", "projected")

  }

  override def transformSchema(schema: StructType): StructType = {

    val features = $(featuresCol)
    val label = $(labelCol)

    val fnames = schema.fieldNames

    if (!fnames.contains(features))
      throw new IllegalArgumentException(s"Features column '$features' does not exist.")

    if (fnames.contains(label))
      throw new IllegalArgumentException(s"Label column '$label' does not exist.")
    /*
     * The current implementation does not change the
     * provided schema of the dataframe
     */
    schema
  }

}
