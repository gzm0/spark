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

trait GlobalAddons {

  /** Naming conventions of the REPL we are working in. */
  def naming: interpreter.Naming

  val global: Global

  import global._
  import rootMirror._

  lazy val sessionNames = naming.sessionNames

  lazy val usesReplObjectAnnot = getRequiredClass("org.apache.spark.repl.UsesReplObjects")

  def isReplState(sym: Symbol): Boolean =
    (sym.isVal || sym.isVar) && isReplSym(sym, topLevel = true)

  def canCaptureReplState(sym: Symbol): Boolean = {
    sym.isMethod && !sym.isConstructor && isReplSym(sym, topLevel = true) ||
    (sym.isClass || sym.isTrait) && !sym.isModuleClass &&
    // hack to avoid lazy val deadlock upon init
    sym.fullName != "$repl_$init" &&
    isReplSym(sym, topLevel = false)
  }

  private def isReplSym(sym: Symbol, topLevel: Boolean) = {
    import sessionNames._

    val parts = sym.fullName.split('.')
    val partsCount = parts.length

    def isLineName(str: String) =
      str.startsWith(line) &&
      scala.util.Try(str.stripPrefix(line).toInt).isSuccess

    @annotation.tailrec
    def isWrapOnly(str: String): Boolean = {
      if (str.startsWith(read)) {
        isWrapOnly(str.stripPrefix(read))
      } else if (str.startsWith("$$iw")) {
        isWrapOnly(str.stripPrefix("$$iw"))
      } else {
        str.isEmpty
      }
    }

    (partsCount >= 3) &&
    isLineName(parts(0)) &&
    parts(1) == read &&
    (!topLevel || parts.slice(2, partsCount - 1).forall(_ == "$iw"))
  }

  def failOnFunction(phaseName: String, tree: Function): Nothing = {
    throw new AssertionError(
        s"$phaseName assumes no Function trees are around. This is a bug in " +
        "the Spark REPL. Please report this together with the session that caused this.")
  }

}
