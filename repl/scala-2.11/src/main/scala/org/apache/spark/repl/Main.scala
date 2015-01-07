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

import org.apache.spark.util.Utils
import org.apache.spark._

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.ILoop

object Main extends Logging {

  val conf = new SparkConf()
  val tmp = System.getProperty("java.io.tmpdir")
  val rootDir = conf.get("spark.repl.classdir", tmp)
  val outputDir = Utils.createTempDir(rootDir)
  val s = new Settings()
  s.processArguments(List("-Yrepl-class-based",
    "-Yrepl-outdir", s"${outputDir.getAbsolutePath}", "-Yrepl-sync"), true)
  val classServer = new HttpServer(outputDir, new SecurityManager(conf))
  var sparkContext: SparkContext = _
  var interp = new SparkILoop // this is a public var because tests reset it.

  def main(args: Array[String]) {
    if (getMaster == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")
    // Start the classServer and store its URI in a spark system property
    // (which will be passed to executors so that they can connect to it)
    classServer.start()
    s.processArguments(args.toList, true)
    interp.process(s) // Repl starts and goes in loop of R.E.P.L
    classServer.stop()
    Option(sparkContext).map(_.stop)
  }


  def getAddedJars: Array[String] = {
    val envJars = sys.env.get("ADD_JARS")
    val propJars = sys.props.get("spark.jars").flatMap { p => if (p == "") None else Some(p) }
    val jars = propJars.orElse(envJars).getOrElse("")
    Utils.resolveURIs(jars).split(",").filter(_.nonEmpty)
  }

  def createSparkContext(): SparkContext = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val jars = getAddedJars
    val conf = new SparkConf()
      .setMaster(getMaster)
      .setAppName("Spark shell")
      .setJars(jars)
      .set("spark.repl.class.uri", classServer.uri)
    logInfo("Spark class server started at " + classServer.uri)
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }
    sparkContext = new SparkContext(conf)
    logInfo("Created spark context..")
    sparkContext
  }

  private def getMaster: String = {
    val master = {
      val envMaster = sys.env.get("MASTER")
      val propMaster = sys.props.get("spark.master")
      propMaster.orElse(envMaster).getOrElse("local[*]")
    }
    master
  }
  
}

import scala.tools.nsc._
import interpreter._
import scala.language.{ implicitConversions, existentials }
import scala.annotation.tailrec
//import Predef.{ println => _, _ }
import interpreter.session._
import StdReplTags._
import scala.reflect.api.{Mirror, Universe, TypeCreator}
import scala.util.Properties.{ jdkHome, javaVersion, versionString, javaVmName }
import scala.tools.nsc.util.{ ClassPath, Exceptional, stringFromWriter, stringFromStream }
import scala.reflect.{ClassTag, classTag}
import scala.reflect.internal.util.{ BatchSourceFile, ScalaClassLoader }
import ScalaClassLoader._
import scala.reflect.io.{ File, Directory }
import scala.tools.util._
import scala.collection.generic.Clearable
import scala.concurrent.{ ExecutionContext, Await, Future, future }
import ExecutionContext.Implicits._
import java.io.{ BufferedReader, FileReader }

class SparkILoop extends ILoop {

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

    import org.apache.spark.SPARK_VERSION
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

  /** Standard commands **/
  // TODO leave like that?
  /*
  override lazy val standardCommands = List(
    cmd("cp", "<path>", "add a jar or directory to the classpath", addClasspath),
    cmd("edit", "<id>|<line>", "edit history", editCommand),
    cmd("help", "[command]", "print this summary or command-specific help", helpCommand),
    historyCommand,
    cmd("h?", "<string>", "search the history", searchHistory),
    cmd("imports", "[name name ...]", "show import history, identifying sources of names", importsCommand),
    //cmd("implicits", "[-v]", "show the implicits in scope", intp.implicitsCommand),
    cmd("javap", "<path|class>", "disassemble a file or class name", javapCommand),
    cmd("line", "<id>|<line>", "place line(s) at the end of history", lineCommand),
    cmd("load", "<path>", "interpret lines in a file", loadCommand),
    cmd("paste", "[-raw] [path]", "enter paste mode or paste a file", pasteCommand),
    // nullary("power", "enable power user mode", powerCmd),
    nullary("quit", "exit the interpreter", () => Result(keepRunning = false, None)),
    nullary("replay", "reset execution and replay all previous commands", replay),
    nullary("reset", "reset the repl to its initial state, forgetting all session entries", resetCommand),
    cmd("save", "<path>", "save replayable session to a file", saveCommand),
    shCommand,
    cmd("settings", "[+|-]<options>", "+enable/-disable flags, set compiler options", changeSettings),
    nullary("silent", "disable/enable automatic printing of results", verbosity),
//    cmd("type", "[-v] <expr>", "display the type of an expression without evaluating it", typeCommand),
//    cmd("kind", "[-v] <expr>", "display the kind of expression's type", kindCommand),
    nullary("warnings", "show the suppressed warnings from the most recent line which had any", warningsCommand)
  )*/
}
