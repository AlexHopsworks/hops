/*
 * Copyright 2019 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.hdfs.TestFileCreation.createFile;
import static org.apache.hadoop.hdfs.TestFileCreation.seed;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class TestFSProvenance {

  static final int blockSize = 8192;
  boolean simulatedStorage = false;

  @Test
  public void testFileCreateAndDelete() throws IOException {
    Configuration conf = new HdfsConfiguration();

    final int NUM_FILES = 5;
    final int NUM_REPLICAS = 1;

    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_REPLICAS).build();
    DistributedFileSystem dfs = cluster.getFileSystem();
    cleanDirStructure(dfs);
    Path dir = createDirStructure(dfs);

    try {
      for (int i = 0; i < NUM_FILES; i++) {

        // create a new file in home directory. Do not close it.
        Path file = new Path(dir, "file_" + i + ".dat");
        FSDataOutputStream stm = createFile(dfs, file, NUM_REPLICAS);

        // verify that file exists in FS namespace
        assertTrue(file + " should be a file", dfs.getFileStatus(file).isFile());

        stm.close();
      }
    } finally {
      cleanDirStructure(dfs);
      cluster.shutdown();
    }
  }

  @Test
  public void testFileReadWrite() throws IOException {
    Configuration conf = new HdfsConfiguration();

    final int NUM_BLOCKS_PER_FILE = 5;
    final int NUM_REPLICAS = 1;

    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_REPLICAS).build();
    DistributedFileSystem dfs = cluster.getFileSystem();
    cleanDirStructure(dfs);
    Path dir = createDirStructure(dfs);

    try {
      // create a new file in home directory. Do not close it.
      Path file = new Path(dir, "file.dat");
      FSDataOutputStream dos = createFile(dfs, file, NUM_REPLICAS);

      // verify that file exists in FS namespace
      assertTrue(file + " should be a file", dfs.getFileStatus(file).isFile());

      // write to file
      byte[] buffer = AppendTestUtil.randomBytes(seed, blockSize * NUM_BLOCKS_PER_FILE);
      dos.write(buffer, 0, blockSize * NUM_BLOCKS_PER_FILE);

      dos.close();

      FSDataInputStream dis = dfs.open(file);
      byte[] actual;
      actual = new byte[blockSize];
      dis.readFully(0, actual);
      actual = new byte[blockSize];
      dis.readFully(blockSize, actual);
      dis.close();
    } finally {
      cleanDirStructure(dfs);
      cluster.shutdown();
    }
  }

  private Path createDirStructure(DistributedFileSystem dfs) throws IOException {
    Path projects = new Path("/Projects");
    dfs.mkdir(projects, FsPermission.getDefault());
    Path project = new Path(projects, "project");
    dfs.mkdir(project, FsPermission.getDefault());
    final Path dataset = new Path(project, "dataset");
    dfs.mkdir(dataset, FsPermission.getDefault());
    DFSClient dfsClient = dfs.getClient();
    dfsClient.setMetaEnabled(dataset.toString(), true);
    final Path subdir = new Path(dataset, "subdir");
    dfs.mkdir(subdir, FsPermission.getDefault());
    return subdir;
  }

  private void cleanDirStructure(DistributedFileSystem dfs) throws IOException {
    Path projects = new Path("/Projects");
    Path project = new Path(projects, "project");
    final Path dataset = new Path(project, "dataset");
    final Path subdir = new Path(dataset, "subdir");
    dfs.delete(subdir, true);
    dfs.delete(dataset, true);
    dfs.delete(project, true);
  }
}
