/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.core.monitoring.Listener
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

abstract class SparkUtil extends FunSuite {
  lazy val sc = SparkUtil.sc
}

object SparkUtil {
  private lazy val sc = {
    val conf = new SparkConf().setAppName("scalaTest").setMaster("local[2]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)

    val sc = new SparkContext(conf)
    sc.addSparkListener(new Listener)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc
  }
}
