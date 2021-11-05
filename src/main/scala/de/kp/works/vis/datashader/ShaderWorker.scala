package de.kp.works.vis.datashader

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
import scala.collection.mutable
import scala.sys.process._
import scala.util.Try

class ShaderWorker {

  /**
   * We expect that the overall configuration is initialized
   * when build a [ShaderWorker]. If this is not the case here,
   * we continue with the internal configuration
   */
  if (!ShaderConf.isInit) ShaderConf.init()
  /**
   * Extract the base folder where the `datashader` project is
   * located
   */
  private val shaderFolder = ShaderConf.getCfg.get
    .getString("shaderFolder")
  /**
   * Extract the Python version to use with `sigma`.
   */
  private val pythonVersion = ShaderConf.getCfg.get
    .getInt("pythonVersion")

  private val pythonPackageVersionRegex = "^Python ([0-9]*)\\.([0-9]*)\\.([0-9]*)".r

  private val versionCheck = checkPythonVersion(pythonVersion)

  if (versionCheck.isDefined) {
    val (major, _, _) = versionCheck.get
    if (major != pythonVersion)
      throw new Exception(s"Installed Python version `$pythonVersion` is different from configured one.")
  }
  else
    throw new Exception(s"Python version `$pythonVersion` is not available.")

  private def checkPythonVersion(pythonVersion: Int): Option[(Int, Int, Int)] =
    Try {
      s"python$pythonVersion --version"
        .lineStream
        .collectFirst {
          case pythonPackageVersionRegex(major, minor, patch) => (major.toInt, minor.toInt, patch.toInt)
        }
     }.getOrElse(None)

   def run(command:String):Unit = {

    val cmdline = s"python$pythonVersion $command"
    val output = mutable.ArrayBuffer.empty[String]
    try {
      /*
       * Call python program to visualize configured
       * *.parquet file data
       */
      val thread = new Thread {

        override def run() {

          val output = mutable.ArrayBuffer.empty[String]
          cmdline
            .lineStream
            .foreach(s => output += s)

          output.foreach(println)

        }

      }

      thread.start()

    } catch {
      case t:Throwable => /* Do nothing */
        t.printStackTrace()
    }

    output

  }

}
