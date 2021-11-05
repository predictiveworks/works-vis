package de.kp.works.vis.datashader.actor

/*
 * Copyright (c) 2012 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.http.scaladsl.model.HttpRequest
import com.google.gson.JsonObject
import de.kp.works.vis.datashader.ShaderWorker

class ShaderActor() extends BaseActor {

  /**
   * Extract the base folder where the `datashader`
   * project is located
   */
  private val shaderFolder: String = sigmaCfg
    .getString("shaderFolder")

  private val worker = new ShaderWorker()
  /**
   * This actor executes a Sigma rule conversion
   * request and returns the response for the
   * specified target
   */
  override def execute(request: HttpRequest): String = {

    val json = getBodyAsJson(request)
    if (json == null) {
      log.error("Request did not contain valid JSON.")
      return null
    }

    // TODO
    val payload = json.getAsJsonObject

    val response = new JsonObject
     response.toString

  }

}
