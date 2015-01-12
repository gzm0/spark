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

import scala.tools.nsc._
import scala.tools.nsc.plugins._

abstract class SparkREPLPlugin(val global: Global) extends Plugin {
  import global._

  /** Naming scheme of the REPL. Abstract since it must be very lazy */
  def naming: interpreter.Naming

  val name = "spark-repl"
  val description = "Spark REPL Plugin"
  val components: List[PluginComponent] = List(
      StateClosureRecorderComponent,
      StateClosureTransformerComponent
  )

  object addons extends {
    val global: SparkREPLPlugin.this.global.type = SparkREPLPlugin.this.global
  } with GlobalAddons {
    def naming: interpreter.Naming = SparkREPLPlugin.this.naming
  }



  object StateClosureRecorderComponent extends {
    val global: SparkREPLPlugin.this.global.type = SparkREPLPlugin.this.global
    val addons: SparkREPLPlugin.this.addons.type = SparkREPLPlugin.this.addons
    // We need to run after uncurry to have lambdas in their class form
    override val runsAfter = List("uncurry")
    override val runsBefore = List("explicitouter")
  } with StateClosureRecorder

  object StateClosureTransformerComponent extends {
    val global: SparkREPLPlugin.this.global.type = SparkREPLPlugin.this.global
    val addons: SparkREPLPlugin.this.addons.type = SparkREPLPlugin.this.addons
    override val runsAfter = List("spark-closurerecord")
    override val runsBefore = List()
  } with StateClosureTransformer

}
