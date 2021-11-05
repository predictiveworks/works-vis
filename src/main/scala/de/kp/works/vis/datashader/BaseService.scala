package de.kp.works.vis.datashader

/*
 * Copyright (c) 2020 Dr. Krusche & Partner PartG. All rights reserved.
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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.Config
import de.kp.works.vis.ssl.SslOptions

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

trait BaseService {

  private var server:Option[Future[Http.ServerBinding]] = None

  /**
   * Akka 2.6 provides a default materializer out of the box, i.e., for Scala
   * an implicit materializer is provided if there is an implicit ActorSystem
   * available. This avoids leaking materializers and simplifies most stream
   * use cases somewhat.
   */
  implicit val system: ActorSystem = ActorSystem("datashader-service")
  implicit lazy val context: ExecutionContextExecutor = system.dispatcher

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  /**
   * Common timeout for all Akka connection
   */
  implicit val timeout: Timeout = Timeout(5.seconds)

  def buildRoute(cfg:Config):Route

  def start(conf:Option[String]):Unit = {

    ShaderConf.init(conf)

    if (!ShaderConf.isInit) {

      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Loading configuration failed and service is not started.")

    }

    val cfg = ShaderConf.getCfg.get

    val routes = buildRoute(cfg)
    val binding = cfg.getConfig("binding")

    val host = binding.getString("host")
    val port = binding.getInt("port")

    val security = cfg.getConfig("security")
    server =
      if (security.getString("ssl") == "false")
        Some(Http().bindAndHandle(routes , host, port))

      else {
        val context = SslOptions.buildServerConnectionContext(security)
        Some(Http().bindAndHandle(routes, host, port, connectionContext = context))
      }

    /* After start processing */
    onStart(cfg)

  }

  def onStart(cfg:Config):Unit

  def stop():Unit = {

    if (server.isEmpty)
      throw new Exception("Service was not launched.")

    server.get
      /*
       * Trigger unbinding from port
       */
      .flatMap(_.unbind())
      /*
       * Shut down application
       */
      .onComplete(_ => {
        system.terminate()
      })

  }

}
