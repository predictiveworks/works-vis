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

import org.apache.spark.sql.SparkSession

object Session {

  private var session: Option[SparkSession] = None

  /**
   * The SparkSession is built without using the respective
   * builder as this approach can be used e.g. to integrate
   * Analytics-Zoo additional context with ease.
   */
  def initialize(): Unit = {

      val spark = SparkSession.builder
        .appName("WorksVis")
        .master("local[4]")
        .config("spark.driver.memory"       , "4g")
        .config("spark.driver.maxResultSize", "12g")
        .config("spark.executor.memory"     , "12g")
        .getOrCreate()

    session = Option(spark)

  }

  def getSession: SparkSession = {
    if (session.isEmpty) initialize()
    session.get
  }

}
