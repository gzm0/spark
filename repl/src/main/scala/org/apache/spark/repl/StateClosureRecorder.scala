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

  class StateClosureRecorderTransformer extends Transformer {
    private type UsageRecord = mutable.Map[Symbol, mutable.Buffer[Symbol]]

    private[this] var currentClosure: Symbol = NoSymbol

    private[this] val instantiatedClasses: UsageRecord = mutable.Map.empty
    private[this] val usedTerms: UsageRecord = mutable.Map.empty

    override def transform(tree: Tree): Tree = {
      val sym = tree.symbol
      tree match {
        case defDef: DefDef if canCaptureReplState(sym) =>
          withClosure(sym) { transform(defDef.rhs) }
          storeUsage(sym)
          tree

        case classDef: ClassDef if canCaptureReplState(sym) =>
          withClosure(sym) { transform(classDef.impl) }
          storeUsage(sym)
          tree

        case sel: Select if isReplState(sym) =>
          recordUsage(sym, usedTerms)
          tree

        case sel: Select if canCaptureReplState(sym) =>
          transform(sel.qualifier)
          recordUsage(tree.symbol, usedTerms)
          tree

        case New(tpt) if canCaptureReplState(tpt.symbol) =>
          recordUsage(tpt.symbol, instantiatedClasses)
          tree

        case fun: Function =>
          failOnFunction(phaseName, fun)

        case _ =>
          super.transform(tree)
      }
    }

    private def storeUsage(trgSym: Symbol) = {
      val args =
        instantiatedClasses.getOrElse(trgSym, Nil).map(sym => TypeTree(sym.tpe)) ++
        usedTerms.getOrElse(trgSym, Nil).map(Ident(_))

      val typedArgs = args.map(typer.typed)

      trgSym.addAnnotation(usesReplObjectAnnot, typedArgs: _*)
    }

    private def recordUsage(sym: Symbol, target: UsageRecord) = {
      if (currentClosure != NoSymbol) {
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
