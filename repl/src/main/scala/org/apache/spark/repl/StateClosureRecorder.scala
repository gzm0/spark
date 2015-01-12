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

trait StateClosureRecorder extends plugins.PluginComponent
                               with transform.Transform {

  val addons: GlobalAddons {
    val global: StateClosureRecorder.this.global.type
  }

  import global._
  import rootMirror._
  import addons._

  val phaseName = "spark-closurerecord"

  override def newPhase(p: nsc.Phase) = new StateClosureRecorderPhase(p)

  class StateClosureRecorderPhase(prev: nsc.Phase) extends Phase(prev) {
    override def name = phaseName
    override def description =
      "Record captured REPL state in top level methods and serializable classes"
  }

  override protected def newTransformer(unit: CompilationUnit) =
    new StateClosureRecorderTransformer

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



  class StateClosureRecorderTransformer extends Transformer {

    type UsageRecord = mutable.Map[Symbol, mutable.Buffer[Symbol]]

    var currentClosure: Symbol = _

    val instantiatedClasses: UsageRecord = mutable.Map.empty
    val usedState: UsageRecord = mutable.Map.empty
    val usedMethods: UsageRecord = mutable.Map.empty

    override def transform(tree: Tree): Tree = tree match {
      case DefDef(_, _, _, _, _, rhs) if isReplTopLevel(tree.symbol) =>
        val sym = tree.symbol

        withClosure(sym)(transform(rhs))
        storeUsage(sym)

        tree

      case ClassDef(_, _, _, impl)
          if tree.symbol.isSerializable && !tree.symbol.isModuleClass =>

        val sym = tree.symbol

        withClosure(sym)(transform(impl))
        storeUsage(sym)

        tree

      case sel: Select if isReplTopLevel(tree.symbol) =>
        val sym = tree.symbol

        if (sym.isVal || sym.isVar) {
          recordUsage(sym, usedState)
        } else if (sym.isMethod) {
          recordUsage(sym, usedMethods)
        }

        tree

      case New(tpt) if isReplDefinedClass(tpt.symbol) =>
        recordUsage(tpt.symbol, instantiatedClasses)

        tree

      case fun: Function =>
        failOnFunction(phaseName, fun)

      case _ =>
        super.transform(tree)
    }

    private def storeUsage(trgSym: Symbol) = {
      val args =
        instantiatedClasses.getOrElse(trgSym, Nil).map(sym => TypeTree(sym.tpe)) ++
        usedState.getOrElse(trgSym, Nil).map(Ident(_)) ++
        usedMethods.getOrElse(trgSym, Nil).map(Ident(_))

      val typedArgs = args.map(typer.typed)

      trgSym.addAnnotation(usesReplObjectAnnot, typedArgs: _*)
    }

    private def recordUsage(sym: Symbol, target: UsageRecord) = {
      if (currentClosure != null) {
        target.getOrElseUpdate(currentClosure, mutable.Buffer.empty) += sym
      }
    }

    private def withClosure[T](closure: Symbol)(body: => T) = {
      val oldClosure = currentClosure
      currentClosure = closure
      try body
      finally currentClosure = oldClosure
    }
  }

}
