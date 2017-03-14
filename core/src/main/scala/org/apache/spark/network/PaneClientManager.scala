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

import org.apache.spark.internal.Logging
import paneclient._

object PaneClientManager {

  private var paneClient: PaneClient = null
  private val GOAL_FINISH_TRANSFER_MS = 500
  private var minFlowBytes = 10000000

  private def obtainPaneClient(): PaneClient = synchronized {
    if (paneClient == null) {
      minFlowBytes = System.getProperty("pane_min_flow_bytes", minFlowBytes.toString).toInt
      val hostName = System.getProperty("pane_hostname")
      val port = System.getProperty("pane_port", "4242").toInt
      paneClient = new PaneClientImpl(InetAddress.getByName(hostName), port)
      paneClient.authenticate("root")
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
    if (bytes < minFlowBytes) {
      return
    }

    val disablePane = try {
      System.getProperty("pane_disable", "false").toBoolean
    } catch {
      case _: Exception => false
    }

    if (disablePane) {
      return
    }

    val defaultBandWidthMbytesPS = (bytes * 1000 /* ms */ / GOAL_FINISH_TRANSFER_MS) / 1000000
    val bandwidthMbytesPS: Long = try {
      System.getProperty("pane_bandwidth_reservation_mbytesps", defaultBandWidthMbytesPS.toString)
        .toLong
    } catch {
      case _: Exception => defaultBandWidthMbytesPS
    }

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
      val reservation = new PaneReservation(bandwidthMbytesPS.toInt, flowGroup, start, end)

      obtainPaneClient().getRootShare.reserve(reservation)
      logging.logInfo(s"PANE reservation complete: ($srcHost:$srcPort-$trgHost:$trgPort" +
        s"-$bandwidthMbytesPS) for $bytes bytes to transfer in $GOAL_FINISH_TRANSFER_MS ms.")
    } catch {
      case t: Throwable =>
        logging.logInfo(s"PANE reservation failed: ($srcHost:$srcPort-$trgHost:$trgPort" +
          s"-$bandwidthMbytesPS) for $bytes bytes to transfer in $GOAL_FINISH_TRANSFER_MS ms.", t)
    }
  }
}
