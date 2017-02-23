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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import paneclient._

object PaneClientManager {
  private var paneClient: PaneClient = null
  private var shares = 0

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
                 trgHost: String, trgPort: Int, logging: Logging): Unit = {
    // TODO Only call this with big flows
    notifyFlow(InetAddress.getByName(srcHost), srcPort,
      InetAddress.getByName(trgHost), trgPort, logging)
  }

  def notifyFlow(srcHost: InetAddress, srcPort: Int,
                 trgHost: InetAddress, trgPort: Int, logging: Logging): Unit = {
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

      val bandwidthBitsPerSec = 1e8.toInt
      // TODO Don't hardcode this
      val start = new PaneRelativeTime
      start.setRelativeTime(0)
      // Now
      val end = new PaneRelativeTime
      end.setRelativeTime(5000) // In 5000 ms

      val reservation = new PaneReservation(bandwidthBitsPerSec, flowGroup, start, end)
      val share = new PaneShare(s"$srcHost:$srcPort-$trgHost:${trgPort}_${shares += 1}",
        Int.MaxValue, flowGroup)
      share.setClient(obtainPaneClient())
      share.reserve(reservation)
      logging.logInfo(s"PANE reservation complete. ($srcHost:$srcPort-$trgHost:$trgPort)")
    } catch {
      case e: Exception =>
        logging.logInfo(s"PANE reservation failed. ($srcHost:$srcPort-$trgHost:$trgPort)", e)
    }
  }
}
