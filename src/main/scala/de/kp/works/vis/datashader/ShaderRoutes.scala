package de.kp.works.vis.datashader

/*
 * Copyright (c) 2021 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.{ByteString, Timeout}
import de.kp.works.vis.datashader.actor.BaseActor._
import de.kp.works.vis.http.CORS

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.{Failure, Success}

case class FileSource(fileName: String, source: Source[ByteString, Any])

object ShaderRoutes {
  val SHADER_ACTOR = "shader_actor"
}

class ShaderRoutes(actors:Map[String, ActorRef])(implicit system: ActorSystem) extends CORS {

  implicit lazy val context: ExecutionContextExecutor = system.dispatcher
  /**
 	 * Common timeout for all Akka connections
   */
  implicit val timeout: Timeout = Timeout(5.seconds)

  import ShaderRoutes._
  /**
   * We expect that the overall configuration is initialized
   * when we build [SigmaRoutes]. If this is not the case
   * here, we continue with the internal configuration
   */
  if (!ShaderConf.isInit) ShaderConf.init()

  private val shaderActor = actors(SHADER_ACTOR)

  def shaderVisualize:Route =
    path("visualize" / "image") {
      post {
        extractShaderVisualize
      }
    }

  private def extractShaderVisualize = extract(shaderActor)

  private def extract(actor:ActorRef) = {
    extractRequest { request =>
      complete {
        /*
         * The Http(s) request is sent to the respective
         * actor and the actor' response is sent to the
         * requester as response.
         */
        val future = actor ? request
        Await.result(future, timeout.duration) match {
          case Response(Failure(e)) =>
            val message = e.getMessage
            jsonResponse(message)
          case Response(Success(answer)) =>
            val message = answer.asInstanceOf[String]
            jsonResponse(message)
        }
      }
    }
  }

  private def jsonResponse(message:String) = {

    val length = message.getBytes.length

    val headers = List(
      `Content-Type`(`application/json`),
      `Content-Length`(length)
    )

    HttpResponse(
      status=StatusCodes.OK,
      headers = headers,
      entity = ByteString(message),
      protocol = HttpProtocols.`HTTP/1.1`)

  }

}
