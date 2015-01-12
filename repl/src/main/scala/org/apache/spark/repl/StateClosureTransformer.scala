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

import scala.tools.nsc
import scala.tools.nsc._

import scala.collection.mutable

trait StateClosureTransformer extends plugins.PluginComponent
                                  with transform.Transform {

  val addons: GlobalAddons {
    val global: StateClosureTransformer.this.global.type
  }

  import global._
  import rootMirror._
  import addons._

  val phaseName = "spark-closuretransform"

  override def newPhase(p: nsc.Phase) = new StateClosureTransformerPhase(p)

  class StateClosureTransformerPhase(prev: nsc.Phase) extends Phase(prev) {
    override def name = phaseName
    override def description =
      "Transform classes / methods to record captured REPL state"
  }

  override protected def newTransformer(unit: CompilationUnit) =
    new StateClosureTransformer

  // Cases to take care of:

  /*
   * var x = 1
   * def foo = x + 1
   *
   * // Send this
   * case class A(x: Int) { def bar = foo + x }
   *
   *
   * // Create closure on worker
   * sc.par(1 to 10).map(x => new A(x))
   */

  class StateClosureTransformer extends Transformer {

    override def transform(tree: Tree): Tree = tree match {

      case ClassDef(_, _, _, impl)
          if tree.symbol.isSerializable && !tree.symbol.isModuleClass =>
        val neededState = calculateClosedState(tree.symbol)

        println(tree.symbol.annotations)
        println(s"${tree.symbol.fullName} needs state: $neededState")

        transform(impl)

        tree

      case DefDef(_, _, _, _, _, rhs) if isReplTopLevel(tree.symbol) =>
        val neededState = calculateClosedState(tree.symbol)

        println(tree.symbol.annotations)
        println(s"${tree.symbol.fullName} needs state: $neededState")

        transform(rhs)

        tree

      case fun: Function =>
        failOnFunction(phaseName, fun)

      case _ =>
        super.transform(tree)
    }
  }

  def calculateClosedState(sym: Symbol): Set[Symbol] = {
    val seen = mutable.Set.empty[Symbol]
    val neededState = mutable.Set.empty[Symbol]

    def iter(origSym: Symbol): Unit = {
      for {
        annot <- origSym.getAnnotation(usesReplObjectAnnot).toList
        arg   <- annot.args
        trgSym = arg.symbol
        if !seen(trgSym)
      } {
        seen += trgSym

        if (trgSym.isVal || trgSym.isVar)
          neededState += trgSym
        else
          iter(trgSym)
      }
    }

    iter(sym)

    neededState.toSet
  }

}
