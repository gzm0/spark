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

package org.apache.spark.repl

import scala.tools.nsc.interpreter.{ ILoop, JPrintWriter }
import scala.util.Properties.{ javaVersion, versionString, javaVmName }
import org.apache.spark.SPARK_VERSION

import java.io.BufferedReader

class SparkILoop(in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out) {

  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  private def initializeSpark() {
    intp.beQuietDuring {
      command( """
         @transient val sc = {
           val _sc = org.apache.spark.repl.Main.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
               """)
      command("import org.apache.spark.SparkContext._")
    }
  }

  /** Print a welcome message */
  override def printWelcome() {
    initializeSpark()

    echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

}
