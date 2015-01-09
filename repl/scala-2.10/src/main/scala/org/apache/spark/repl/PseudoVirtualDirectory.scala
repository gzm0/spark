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

import java.io.File

import scala.reflect.io._

/**
 * A physical directory that extends VirtualDirectory.
 *
 * This is of course conceptually plain wrong! We need it to fool the REPL into
 * placing compiled files into a physical directory so we can serve them to
 * other Spark nodes.
 */
private class PseudoVirtualDirectory(_dir: File, name: String,
    maybeContainer: Option[VirtualDirectory])
    extends VirtualDirectory(name, maybeContainer) {

  require(_dir.isDirectory,
      "PseudoVirtualDirectory can only be created with a true directory.")

  private val givenPath: Path = _dir

  override def iterator = _dir.listFiles().iterator.map(PlainFile.fromPath(_))

  // Copied from PlainFile
  override def lookupName(name: String, directory: Boolean): AbstractFile = {
    val child = givenPath / name
    if ((child.isDirectory && directory) || (child.isFile && !directory)) {
      new PlainFile(child)
    } else {
      null
    }
  }

  // Copied from AbstractFile
  private def fileOrSubdirectoryNamed(name: String, isDir: Boolean): AbstractFile = {
    val lookup = lookupName(name, isDir)
    if (lookup != null) lookup
    else {
      val jfile = new File(_dir, name)
      if (isDir) jfile.mkdirs() else jfile.createNewFile()
      new PlainFile(jfile)
    }
  }

  override def fileNamed(name: String): AbstractFile =
    fileOrSubdirectoryNamed(name, false)

  override def subdirectoryNamed(name: String): AbstractFile =
    fileOrSubdirectoryNamed(name, true)

  override def clear(): Unit = {
    // Another abuse to use its functions
    val dir = new Directory(_dir)
    dir.deleteRecursively()
    dir.createDirectory()
  }

}
