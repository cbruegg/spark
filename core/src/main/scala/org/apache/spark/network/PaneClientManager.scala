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
  private val shares = new AtomicInteger(0)
  private val MIN_BYTES = 5000
  private val GOAL_FINISH_TRANSFER_MS = 500

  private def obtainPaneClient(): PaneClient = synchronized {
    if (paneClient == null) {
      val hostName = "10.0.0.11"
      // TODO Don't hardcode this
      val port = 4242 // TODO Don't hardcode this
      paneClient = new PaneClientImpl(InetAddress.getByName(hostName), port)
      paneClient.authenticate("username")
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
      disablePane = System.getProperty("disable_pane", "false").toBoolean
    } catch {
      case _: Exception =>
    }

    if (disablePane) {
      return
    }

    try {
      val flowGroup = new PaneFlowGroup
      flowGroup.setSrcHost(srcHost)
      flowGroup.setSrcPort(srcPort)
      flowGroup.setDstHost(trgHost)
      flowGroup.setDstPort(trgPort)
      flowGroup.setTransportProto(PaneFlowGroup.PROTO_TCP)

      val bandwidthBitsPerSec = (bytes * 8 / GOAL_FINISH_TRANSFER_MS) / 1000
      val start = new PaneRelativeTime
      start.setRelativeTime(0)
      // Now
      val end = new PaneRelativeTime
      end.setRelativeTime(GOAL_FINISH_TRANSFER_MS * 2) // In 5000 ms

      val reservation = new PaneReservation(bandwidthBitsPerSec.toInt, flowGroup, start, end)
      val share = new PaneShare(shares.getAndIncrement().toString, Int.MaxValue, flowGroup)
      share.setClient(obtainPaneClient())
      share.reserve(reservation)
      logging.logInfo(s"PANE reservation complete: ($srcHost:$srcPort-$trgHost:$trgPort)")
    } catch {
      case _: Throwable =>
        logging.logInfo(s"PANE reservation failed: ($srcHost:$srcPort-$trgHost:$trgPort)")
    }
  }
}
