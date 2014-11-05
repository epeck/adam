/*
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, Path}

/**
 * A locator for files in HDFS.
 */
class HDFSFileLocator(val conf: Configuration, val path: Path) extends FileLocator {
  override def parentLocator(): Option[FileLocator] =
    Some(new HDFSFileLocator(conf, path.getParent))

  override def relativeLocator(relativePath: String): FileLocator =
    new HDFSFileLocator(conf, new Path(path, relativePath))

  override def bytes: ByteAccess = {
    val length = path.getFileSystem(conf).getFileStatus(path).getLen
    val stream = () => path.getFileSystem(conf).open(path)
    new InputStreamByteAccess(stream, length)
  }

  class HDFSIterable(val conf: Configuration, val remoteIterator: RemoteIterator[LocatedFileStatus]) extends Iterable[FileLocator] {
    override def iterator: Iterator[FileLocator] = new Iterator[FileLocator] {
      override def next(): FileLocator = new HDFSFileLocator(conf, remoteIterator.next().getPath)

      override def hasNext: Boolean = remoteIterator.hasNext
    }
  }

  override def childLocators(): Iterable[FileLocator] =
    new HDFSIterable(conf, path.getFileSystem(conf).listFiles(path, false))
}
