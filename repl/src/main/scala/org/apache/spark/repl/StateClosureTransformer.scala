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

  val prefix = newTermName("$spark$repl$")

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
   *
   * // use a state capturing method on worker:
   * def bar(y: Int) = x + y
   * sc.par(1 to 10).map(bar)
   *
   * // subclassing and traits
   * trait Foo { def foo = x }
   * case class A extends Foo
   */

  class StateClosureTransformer extends Transformer {
    override def transform(tree: Tree): Tree = {
      val sym = tree.symbol
      tree match {
        case classDef: ClassDef if canCaptureReplState(sym) =>
          val neededState = calculateCapturedState(sym)

          if (neededState.nonEmpty && sym.isSerializable) {
            println(s"$sym needs state: $neededState")

            // TODO distinguish class / trait
            patchClass(classDef, neededState)
          } else {
            import classDef._
            val newImpl = transformTemplate(classDef.impl)
            treeCopy.ClassDef(classDef, mods, name, tparams, newImpl)
          }

        case defDef: DefDef if canCaptureReplState(sym) =>
          val neededState = calculateCapturedState(sym)
          val newRhs = transform(defDef.rhs)

          if (neededState.nonEmpty) {
            println(s"$sym needs state: $neededState")
            // TODO add second method which also accepts state
            super.transform(tree)
          } else {
            import defDef._
            treeCopy.DefDef(defDef, mods, name, tparams, vparamss, tpt, newRhs)
          }

        case fun: Function =>
          failOnFunction(phaseName, fun)

        case _ =>
          super.transform(tree)
      }
    }

    private def patchClass(classDef: ClassDef, neededState: Set[Symbol]): ClassDef = {
      val sym = classDef.symbol

      require(sym.isSerializable)

      val superClassState = calculateCapturedState(sym.superClass)

      if (superClassState.nonEmpty && !sym.superClass.isSerializable) {
        val stateString = superClassState.map(_.name).mkString("[", ", ", "]")
        reporter.error(classDef.pos, s"Serializable $sym extends ${sym.superClass} which is not " +
            s"serializable but captures following REPL state: $stateString.")
        classDef
      } else {
        val trulyCapturedState = neededState -- superClassState

        val stateFields = for {
          state <- trulyCapturedState
        } yield {
          val fldSym =
            sym.newVariable(prefix append state.name, NoPosition, Flag.PROTECTED)
          fldSym.setInfo(state.tpe)
          sym.info.decls.enter(fldSym)
        }

        val newTempl = {
          val templ = classDef.impl
          val valDefs = stateFields.map(sym => typer.typed(ValDef(sym))).toList
          val newBody = valDefs ++ templ.body.map(transform)
          treeCopy.Template(templ, templ.parents, templ.self, newBody)
        }

        treeCopy.ClassDef(classDef, classDef.mods, classDef.name, classDef.tparams, newTempl)
      }
    }
  }

  /** Fixpoint of the state needed by a given symbol based on annotation.
   *  A basic call-stack based DFS
   */
  private def calculateCapturedState(sym: Symbol): Set[Symbol] = {
    val seen = mutable.Set.empty[Symbol]
    val neededState = mutable.Set.empty[Symbol]

    def iter(origSym: Symbol): Unit = {
      val annotSyms = for {
        annot <- origSym.getAnnotation(usesReplObjectAnnot).toList
        arg   <- annot.args
      } yield arg.symbol

      for {
        trgSym <- annotSyms ++ sym.parentSymbols
        if !seen(trgSym)
      } {
        seen += trgSym

        if (isReplState(trgSym))
          neededState += trgSym
        else
          iter(trgSym)
      }

    }

    iter(sym)

    neededState.toSet
  }

}
