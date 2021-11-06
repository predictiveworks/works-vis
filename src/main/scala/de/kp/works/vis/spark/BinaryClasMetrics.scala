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

import com.google.gson.JsonObject
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics => Metrics}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * This class is built to write binary classification
 * metrics are *.parquet file with a reference to the
 * specific classifier.
 */
class BinaryClasMetrics(session:SparkSession, folder: String, metrics: Metrics) {
  /**
   * This model extracts the area under the ROC
   * and PR curve and returns these values as
   * a JSON object
   */
  def getReport:JsonObject = {

    val areaUnderROC = metrics.areaUnderROC()
    val areaUnderPR = metrics.areaUnderPR()

    val reportObj = new JsonObject
    reportObj.addProperty("areaUnderROC", areaUnderROC)
    reportObj.addProperty("areaUnderPR", areaUnderPR)

    reportObj

   }
   /**
    * Returns the receiver operating characteristic (ROC)
    * curve, which is an RDD of (false positive rate, true
    * positive rate) with
    * (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
    */
   def getROC:DataFrame = {

     val data = metrics.roc()
       .map{case(fpr, tpr) => Row(fpr, tpr)}

     val schema = StructType(
       StructField("fpr", DoubleType, nullable = false) ::
       StructField("tpr", DoubleType, nullable = false) :: Nil)

     session.createDataFrame(data, schema)

  }
  /**
   * Returns the precision-recall curve, which is an RDD of
   * (recall, precision), NOT (precision, recall),
   *
   * with (0.0, p) prepended to it, where p is the precision
   * associated with the lowest recall on the curve.
   */
  def getPR:DataFrame = {

    val data = metrics.pr()
      .map{case(recall, precision) => Row(recall, precision)}

    val schema = StructType(
      StructField("recall", DoubleType, nullable = false) ::
        StructField("precision", DoubleType, nullable = false) :: Nil)

    session.createDataFrame(data, schema)

  }
  /**
   * Returns the (threshold, F-Measure) curve for
   * the beta factor in F-Measure computation.
   */
  def getFMeasure(beta: Double):DataFrame = {

    val data = metrics.fMeasureByThreshold(beta)
      .map{case(threshold, fmeasure) => Row(threshold, fmeasure)}

    val schema = StructType(
      StructField("threshold", DoubleType, nullable = false) ::
        StructField("fmeasure", DoubleType, nullable = false) :: Nil)

    session.createDataFrame(data, schema)

  }
  /**
   * Returns the (threshold, precision) curve.
   */
  def getPrecision:DataFrame = {

    val data = metrics.precisionByThreshold()
      .map{case(threshold, precision) => Row(threshold, precision)}

    val schema = StructType(
      StructField("threshold", DoubleType, nullable = false) ::
        StructField("precision", DoubleType, nullable = false) :: Nil)

    session.createDataFrame(data, schema)

  }
  /**
   * Returns the (threshold, recall) curve.
   */
  def getRecall:DataFrame = {

    val data = metrics.precisionByThreshold()
      .map{case(threshold, recall) => Row(threshold, recall)}

    val schema = StructType(
      StructField("threshold", DoubleType, nullable = false) ::
        StructField("recall", DoubleType, nullable = false) :: Nil)

    session.createDataFrame(data, schema)

  }
  /**
   * Writes the specified threshold curve of the binary classifier,
   * identified by its model id `mid` as parquet file to the configured
   * metrics folder.
   *
   * This folder is shared the (Python) datashader and enables
   * visualization as a Dash App.
   *
   */
  def writeBinaryThreshold(mid:String, target:String, beta:Option[Double]=None):Unit = {

    if (folder == null)
      throw new Exception("No metrics folder provided.")

    val path = s"$folder/$target/$mid.parquet"

    target match {
      case "pr" =>
        val dataframe = getPR
        dataframe.write.parquet(path)

      case "row" =>
        val dataframe = getROC
        dataframe.write.parquet(path)

      case "fmeasure" =>
        val b = if (beta.isDefined) beta.get else 1.0

        val dataframe = getFMeasure(b)
        dataframe.write.parquet(path)

      case "precision" =>
        val dataframe = getPrecision
        dataframe.write.parquet(path)

      case "recall" =>
        val dataframe = getRecall
        dataframe.write.parquet(path)

      case _ =>
        throw new Exception(s"Unknown target for threshold curve detected.")
    }

  }

}
