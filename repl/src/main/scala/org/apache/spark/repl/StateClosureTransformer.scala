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
import scala.tools.nsc.ast.TreeDSL

trait StateClosureTransformer extends plugins.PluginComponent
                                  with transform.Transform
                                  with ast.TreeDSL {

  val addons: GlobalAddons {
    val global: StateClosureTransformer.this.global.type
  }

  import global._
  import rootMirror._
  import addons._
  import definitions._

  val phaseName = "spark-closuretransform"

  object sparkNme {
    private val basePrefix = newTermName("spark$repl$")
    private val statePrefix = basePrefix.append("state$")
    private val accPrefix = basePrefix.append("acc$")

    def stateField(sym: Symbol): TermName =
      statePrefix.append(nme.localToGetter(sym.name))

    def stateAccessor(sym: Symbol): TermName =
      accPrefix.append(nme.localToGetter(sym.name))

    val deserialized: TermName = basePrefix.append("deserialized")
  }

  object javaSer {
    object nme {
      val writeObject = newTermName("writeObject")
      val readObject = newTermName("readObject")
      val readObjectNoData = newTermName("readObjectNoData")
      val writeReplace = newTermName("writeReplace")
      val readResolve = newTermName("readResolve")
      val out = newTermName("out")
      val in = newTermName("in")
    }

    object definitions {
      lazy val ObjectOutputStreamClass = getRequiredClass("java.io.ObjectOutputStream")
      lazy val ObjectOutputStreamTpe = ObjectOutputStreamClass.tpe
      lazy val ObjectOutputStream_defaultWriteObject =
        getMemberMethod(ObjectOutputStreamClass, newTermName("defaultWriteObject"))

      lazy val ObjectInputStreamClass = getRequiredClass("java.io.ObjectInputStream")
      lazy val ObjectInputStreamTpe = ObjectInputStreamClass.tpe
      lazy val ObjectInputStream_defaultReadObject =
        getMemberMethod(ObjectInputStreamClass, newTermName("defaultReadObject"))
    }
  }

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
   *
   * // using state in ctor
   *
   * // subclass of class that has custom writeObject / readObject
   *
   *
   */

   // Test cases

   /*
    * var x = 1
    * case class A(y: Int) { def sum = x + y }
    * val as = List.tabulate(10)(new A(_))
    * sc.parallelize(as).map(_.sum).reduce(_ + _)
    * x = 2
    * sc.parallelize(as).map(_.sum).reduce(_ + _)
    */

  class StateClosureTransformer extends Transformer {
    private[this] var currentPatchClass: Symbol = NoSymbol
    private def isPatching = currentPatchClass != NoSymbol

    override def transform(tree: Tree): Tree = {
      val sym = tree.symbol
      tree match {
        case classDef: ClassDef if canCaptureReplState(sym) =>
          val neededState = calculateCapturedState(sym)

          if (neededState.nonEmpty && sym.isSerializable) {
            println(s"$sym needs state: $neededState")
            if (sym.isClass) {
              patchClass(classDef, neededState)
            } else if (sym.isTrait) {
              // FIXME support this
              reporter.error(sym.pos, "Traits capturing state are not yet supported")
              super.transform(tree)
            } else {
              throw new AssertionError(
                  "Found non-class and non-trait marked to capture state: " + sym.fullName)
            }
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

        case Apply(_, args) if isPatching && canCaptureReplState(sym) =>
          // FIXME find a way to evaluate receiver

          /* Note that when transforming accesses to calls to top-level REPL
           * members, we can safely discard the receiver, since it is a
           * REPL wrapper. No one can (should) write a side-effecting method
           * that returns a REPL wrapper.
           */
          val neededState = calculateCapturedState(sym)

          val isStateAccessor = sym.isAccessor && neededState.size == 1 && {
            val accessed = sym.accessed
            isReplState(accessed) && neededState.head == accessed
          }

          if (isStateAccessor) {
            import CODE._

            assert(args.isEmpty, "Accessor with non-empty argument list!")

            val stateField = neededState.head
            val accessorName = sparkNme.stateAccessor(stateField)
            val stateAccessor = getMemberMethod(currentPatchClass, accessorName)

            assert(stateField.isStatic, "Found non-static state field")

            REF(stateAccessor)
          } else {
            // TODO make sure no one sets worksheet state when not on master
            super.transform(tree) // TODO do general transform
          }


        case fun: Function =>
          failOnFunction(phaseName, fun)

        case _ =>
          super.transform(tree)
      }
    }

    private def patchClass(classDef: ClassDef, neededState: Set[Symbol]): ClassDef = {
      val clsSym = classDef.symbol

      require(clsSym.isSerializable)

      val superSym = clsSym.superClass
      val superClassState = calculateCapturedState(superSym)

      if (superClassState.nonEmpty && !superSym.isSerializable) {
        val stateNames = superClassState.map(s => nme.localToGetter(s.name))
        val stateString = stateNames.mkString("[", ", ", "]")

        reporter.error(classDef.pos,
            s"Serializable $clsSym extends $superSym which is not " +
            s"serializable but captures following REPL state: $stateString.")
        classDef
      } else {
        val trulyCapturedState = (neededState -- superClassState).toList

        // Generate the is deserialized field if necessary
        val deserializedField =
          if (superClassState.nonEmpty) Nil
          else List(genSerializedField(clsSym))

        // Generate state members
        val stateMembers = genStateMembers(clsSym, trulyCapturedState)

        // Generate serialization interceptors
        val intercepts = genSerializationIntercepts(clsSym, trulyCapturedState)

        // Patch the calls to state in the body of the class
        val template = classDef.impl
        val patchedBody = withPatchingClass(clsSym) {
          transformStats(template.body, template.symbol)
        }

        // We have our new class body!
        val newBody = (
            deserializedField ++
            stateMembers ++
            intercepts ++
            patchedBody)

        // Put it all together
        val newTempl = treeCopy.Template(template,
            transformTrees(template.parents),
            transformValDef(template.self),
            newBody)

        treeCopy.ClassDef(classDef, classDef.mods, classDef.name,
            classDef.tparams, newTempl)
      }
    }

    private def withPatchingClass[T](patchClass: Symbol)(body: => T): T = {
      val oldPatchClass = currentPatchClass
      currentPatchClass = patchClass
      try body
      finally currentPatchClass = oldPatchClass
    }
  }

  private def genSerializedField(clsSym: Symbol): Tree = {
    import CODE._
    val fieldSym = addField(clsSym, sparkNme.deserialized, BooleanTpe)
    fieldSym.addAnnotation(TransientAttr)
    VAL(fieldSym) === LIT(false)
  }

  private def genStateMembers(clsSym: Symbol, capturedState: List[Symbol]): List[Tree] = {
    val stateFieldSyms = for {
      state <- capturedState
    } yield {
      val fieldName = sparkNme.stateField(state)
      addField(clsSym, fieldName, state.tpe)
    }

    val stateFieldTrees = for {
      fieldSym <- stateFieldSyms
    } yield {
      import CODE._
      VAL(fieldSym) === EmptyTree
    }

    // Lookup the deserialized field. It might be in the superclass
    val deserializedField = getMemberValue(clsSym, sparkNme.deserialized)

    val stateAccessorTrees = for {
      (state, field) <- capturedState zip stateFieldSyms
    } yield {
      import CODE._

      val accName = sparkNme.stateAccessor(state)
      val accSym = addAccessor(clsSym, accName, state.tpe)

      DEF(accSym) === {
        IF (REF(deserializedField)) THEN {
          REF(field)
        } ELSE {
          assert(state.isStatic, "Encountered non-static state field")
          REF(accessorOf(state))
        }
      }
    }

    stateFieldTrees ++ stateAccessorTrees
  }

  private def genSerializationIntercepts(clsSym: Symbol,
      capturedState: List[Symbol]): List[Tree] = {
    verifyNoCustomSerialization(clsSym)
    List(genWriteObject(clsSym, capturedState), genReadObject(clsSym))
  }

  /**
   * Verify that a class does not have methods that customize Java serialization
   *
   * Reports an error for each method that exists.
   */
  private def verifyNoCustomSerialization(clsSym: Symbol): Unit = {
    import javaSer.nme._
    import javaSer.definitions._

    def checkMethod(name: TermName)(paramTypes: Symbol*)(retSym: Symbol) = {
      val offendingSym = clsSym.tpe.declaration(name).suchThat { sym =>
        sym.isMethod &&
        sym.tpe.resultType.typeSymbol == retSym &&
        sym.tpe.paramTypes.map(_.typeSymbol) == paramTypes
      }

      if (offendingSym != NoSymbol) {
        reporter.error(offendingSym.pos,
            "A class that captures REPL state may not contain " +
            "methods that alter (Java) serialization behavior")
      }
    }

    checkMethod(writeObject)(ObjectOutputStreamClass)(UnitClass)
    checkMethod(readObject)(ObjectInputStreamClass)(UnitClass)
    checkMethod(readObjectNoData)()(UnitClass)
    checkMethod(writeReplace)()(ObjectClass)
    checkMethod(readResolve)()(ObjectClass)
  }

  private def genWriteObject(clsSym: Symbol, capturedState: List[Symbol]): DefDef = {
    import CODE._

    val meth = clsSym.newMethod(javaSer.nme.writeObject, NoPosition, Flag.PRIVATE)

    val param = meth.newValueParameter(javaSer.nme.out, NoPosition)
    param.setInfo(javaSer.definitions.ObjectOutputStreamTpe)

    meth.setInfo(MethodType(param :: Nil, UnitTpe))

    clsSym.info.decls.enter(meth)

    val assigns = for {
      state <- capturedState
    } yield {
      val field = getMemberValue(clsSym, sparkNme.stateField(state))
      REF(field) === REF(accessorOf(state))
    }

    val serializationCall =
      fn(REF(param), javaSer.definitions.ObjectOutputStream_defaultWriteObject)

    val deserializedField = getMemberValue(clsSym, sparkNme.deserialized)

    DefDef(meth, BLOCK(
        If(NOT(REF(deserializedField)), BLOCK(assigns: _*), EmptyTree),
        serializationCall
     ))
  }

  private def genReadObject(clsSym: Symbol): DefDef = {
    import CODE._

    val meth = clsSym.newMethod(javaSer.nme.readObject, NoPosition, Flag.PRIVATE)

    val param = meth.newValueParameter(javaSer.nme.in, NoPosition)
    param.setInfo(javaSer.definitions.ObjectInputStreamTpe)

    meth.setInfo(MethodType(param :: Nil, UnitTpe))

    clsSym.info.decls.enter(meth)

    DefDef(meth, BLOCK(
        REF(getMemberValue(clsSym, sparkNme.deserialized)) === LIT(true),
        fn(REF(param), javaSer.definitions.ObjectInputStream_defaultReadObject)
    ))
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

  private val memberFlags = Flag.PROTECTED

  private def addField(clsSym: Symbol, name: TermName, tpe: Type): Symbol = {
    val fldSym = clsSym.newVariable(name, NoPosition, memberFlags)
    fldSym.setInfo(tpe)
    clsSym.info.decls.enter(fldSym)
    fldSym
  }

  private def addAccessor(clsSym: Symbol, name: TermName, retTpe: Type): Symbol = {
    val accSym = clsSym.newMethod(name, NoPosition, memberFlags)
    accSym.setInfo(MethodType(Nil, retTpe))
    clsSym.info.decls.enter(accSym)
    accSym
  }

  private def accessorOf(sym: Symbol) =
    getMemberMethod(sym.owner, nme.localToGetter(sym.name))

}
