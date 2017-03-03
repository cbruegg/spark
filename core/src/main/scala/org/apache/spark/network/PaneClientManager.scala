/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import paneclient._

object PaneClientManager {

  private var paneClient: PaneClient = null
  private var rootShare: PaneShare = null
  private val MIN_BYTES = 1000
  private val shares = new AtomicInteger(0)
  private val GOAL_FINISH_TRANSFER_MS = 500

  private def obtainPaneClient(): PaneClient = synchronized {
    if (paneClient == null) {
      val hostName = System.getProperty("pane_hostname")
      val port = System.getProperty("pane_port", "4242").toInt
      paneClient = new PaneClientImpl(InetAddress.getByName(hostName), port)
      paneClient.authenticate("username")

      rootShare = new PaneShare("root", Int.MaxValue, null)
      rootShare.setClient(paneClient)
    }

    paneClient
  }

  def notifyFlow(srcHost: String, srcPort: Int,
                 trgHost: String, trgPort: Int,
                 logging: Logging, bytes: Long): Unit = {
    notifyFlow(InetAddress.getByName(srcHost), srcPort,
      InetAddress.getByName(trgHost), trgPort, logging, bytes)
  }

  def notifyFlow(srcHost: InetAddress, srcPort: Int,
                 trgHost: InetAddress, trgPort: Int,
                 logging: Logging, bytes: Long): Unit = {
    if (bytes < MIN_BYTES) {
      return
    }

    var disablePane: Boolean = false

    try {
      disablePane = System.getProperty("pane_disable", "false").toBoolean
    } catch {
      case _: Exception =>
    }

    if (disablePane) {
      return
    }

    val bandwidthMegaBytesPerSec = (bytes * 1000 /* ms */ / GOAL_FINISH_TRANSFER_MS) / 1000000
    try {
      val flowGroup = new PaneFlowGroup
      flowGroup.setSrcHost(srcHost)
      flowGroup.setSrcPort(srcPort)
      flowGroup.setDstHost(trgHost)
      flowGroup.setDstPort(trgPort)
      flowGroup.setTransportProto(PaneFlowGroup.PROTO_TCP)

      val start = new PaneRelativeTime
      start.setRelativeTime(0)
      // Now
      val end = new PaneRelativeTime
      end.setRelativeTime(GOAL_FINISH_TRANSFER_MS * 2)
      val reservation = new PaneReservation(bandwidthMegaBytesPerSec.toInt, flowGroup, start, end)

      obtainPaneClient()
      rootShare.reserve(reservation)
      logging.logInfo(s"PANE reservation complete: ($srcHost:$srcPort-$trgHost:$trgPort" +
        s"-$bandwidthMegaBytesPerSec)")
    } catch {
      case _: Throwable =>
        logging.logInfo(s"PANE reservation failed: ($srcHost:$srcPort-$trgHost:$trgPort" +
          s"-$bandwidthMegaBytesPerSec)")
    }
  }
}
