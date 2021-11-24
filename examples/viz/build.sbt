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

import sbt.Keys.{libraryDependencies, version}



lazy val root = (project in file(".")).
  settings(
    name := "SedonaVizTemplate",

    version := "0.1.0",

    scalaVersion := "2.12.11",

    organization  := "org.apache.sedona",

    publishMavenStyle := true
  )

val SparkVersion = "3.1.1"

val SparkCompatibleVersion = "3.0"

val HadoopVersion = "2.7.2"

val SedonaVersion = "1.0.1-incubating"

val ScalaCompatibleVersion = "2.12"

// Change the dependency scope to "provided" when you run "sbt assembly"
val dependencyScope = "compile"

val geotoolsVersion = "1.1.0-24.1"

val jacksonVersion = "2.10.0"

logLevel := Level.Warn

logLevel in assembly := Level.Error

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion % dependencyScope,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion % dependencyScope,
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-hdfs" % HadoopVersion % dependencyScope,
  "org.apache.sedona" % "sedona-core-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion,
  "org.apache.sedona" % "sedona-sql-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion ,
  "org.apache.sedona" % "sedona-viz-".concat(SparkCompatibleVersion).concat("_").concat(ScalaCompatibleVersion) % SedonaVersion,
  "org.locationtech.jts"% "jts-core"% "1.18.0" % "compile",
  "org.wololo" % "jts2geojson" % "0.14.3" % "compile", // Only needed if you read GeoJSON files. Under MIT License
  //  The following GeoTools packages are only required if you need CRS transformation. Under GNU Lesser General Public License (LGPL) license
  "org.datasyslab" % "geotools-wrapper" % geotoolsVersion % "compile"
)

assemblyMergeStrategy in assembly := {
  case PathList("org.apache.sedona", "sedona-core", xs@_*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case "META-INF/services" => MergeStrategy.last
  case path if path.endsWith(".SF") => MergeStrategy.discard
  case path if path.endsWith(".DSA") => MergeStrategy.discard
  case path if path.endsWith(".RSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers ++= Seq(
  "Open Source Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/release/",
  "Apache Software Foundation Snapshots" at "https://repository.apache.org/content/groups/snapshots",
  "Java.net repository" at "https://download.java.net/maven/2"
)