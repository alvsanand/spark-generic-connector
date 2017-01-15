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

package es.alvsanand.sgdc.core.util.test

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster

/**
  * Created by alvsanand on 11/01/17.
  */
trait HDFSEmbed {
  private var baseDir: File = null
  private var hdfsCluster: MiniDFSCluster = null

  protected var hfdsPort: Int = 50071

  protected def hdfsStart(port: Int = hfdsPort) = {
    baseDir = Files.createTempDir();

    val conf: Configuration = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())

    val builder: MiniDFSCluster.Builder = new MiniDFSCluster.Builder(conf).nameNodePort(port)

    hdfsCluster = builder.build()
    hdfsCluster.waitActive()
  }

  protected def hdfsStop() = {
    hdfsCluster.shutdown()

    if(!baseDir.delete()) baseDir.deleteOnExit()
  }

  protected def withHDFSFiles(files: List[(String, String)])(fixture: Unit => Unit) {
    if (files!=null) {
      populateHDFSFiles(files)
    }

    fixture()
  }

  private def populateHDFSFiles(files: List[(String, String)]) = {
    val fs: FileSystem = hdfsCluster.getFileSystem

    files.foreach { x => {
      val src = getClass.getResource(x._1)
      val dst = x._2

      if(src==null){
        throw new RuntimeException("FILE[%s] does not exists in classpath".format(x._1))
      }

      fs.copyFromLocalFile(false, new Path(src.toString()), new Path(dst))
    }
    }
  }
}
