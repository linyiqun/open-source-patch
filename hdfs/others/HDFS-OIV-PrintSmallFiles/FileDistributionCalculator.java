/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageTextWriter.InMemoryMetadataDB;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * This is the tool for analyzing file sizes in the namespace image. In order to
 * run the tool one should define a range of integers <tt>[0, maxSize]</tt> by
 * specifying <tt>maxSize</tt> and a <tt>step</tt>. The range of integers is
 * divided into segments of size <tt>step</tt>:
 * <tt>[0, s<sub>1</sub>, ..., s<sub>n-1</sub>, maxSize]</tt>, and the visitor
 * calculates how many files in the system fall into each segment
 * <tt>[s<sub>i-1</sub>, s<sub>i</sub>)</tt>. Note that files larger than
 * <tt>maxSize</tt> always fall into the very last segment.
 *
 * <h3>Input.</h3>
 * <ul>
 * <li><tt>filename</tt> specifies the location of the image file;</li>
 * <li><tt>maxSize</tt> determines the range <tt>[0, maxSize]</tt> of files
 * sizes considered by the visitor;</li>
 * <li><tt>step</tt> the range is divided into segments of size step.</li>
 * </ul>
 *
 * <h3>Output.</h3> The output file is formatted as a tab separated two column
 * table: Size and NumFiles. Where Size represents the start of the segment, and
 * numFiles is the number of files form the image which size falls in this
 * segment.
 *
 */
final class FileDistributionCalculator {
  private final static long MAX_SIZE_DEFAULT = 0x2000000000L; // 1/8 TB = 2^37
  private final static int INTERVAL_DEFAULT = 0x200000; // 2 MB = 2^21
  private final static int MAX_INTERVALS = 0x8000000; // 128 M = 2^27

  private final Configuration conf;
  private final long maxSize;
  private final int steps;
  private final PrintStream out;

  private final int[] distribution;
  private int totalFiles;
  private int totalDirectories;
  private int totalBlocks;
  private long totalSpace;
  private long maxFileSize;

  // Whether print small file infos
  private boolean printSmallFiles;
  private String prefixPath;
  private String[] prefixStrs;
  private HashMap<String, Integer> smallFilesMap;
  private InMemoryMetadataDB metadataMap = null;

  FileDistributionCalculator(Configuration conf, long maxSize, int steps,
      PrintStream out, boolean printSmallFiles, String prefixPath) {
    this.conf = conf;
    this.maxSize = maxSize == 0 ? MAX_SIZE_DEFAULT : maxSize;
    this.steps = steps == 0 ? INTERVAL_DEFAULT : steps;
    this.out = out;
    long numIntervals = this.maxSize / this.steps;
    // avoid OutOfMemoryError when allocating an array
    Preconditions.checkState(numIntervals <= MAX_INTERVALS,
        "Too many distribution intervals (maxSize/step): " + numIntervals +
        ", should be less than " + (MAX_INTERVALS+1) + ".");
    this.distribution = new int[1 + (int) (numIntervals)];
    this.printSmallFiles = printSmallFiles;
    this.prefixPath = prefixPath;
    this.smallFilesMap = new HashMap<String, Integer>();

    if (this.prefixPath != null && this.prefixPath.length() > 0) {
      out.println("PrefixPath: " + this.prefixPath);
      this.prefixStrs = this.prefixPath.split(",");
      out.println("prefixStrs: " + prefixStrs);
    }

    if (this.printSmallFiles) {
      this.metadataMap = new InMemoryMetadataDB();
    }
  }

  void visit(RandomAccessFile file) throws IOException {
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);
    try (FileInputStream in = new FileInputStream(file.getFD())) {
      // If we want to print small files info, we should load directory inodes
      if (printSmallFiles) {
        for (FileSummary.Section s : summary.getSectionsList()) {
          if (SectionName.fromString(s.getName()) == SectionName.INODE) {
            in.getChannel().position(s.getOffset());
            InputStream is =
                FSImageUtil.wrapInputStreamForCompression(conf, summary
                    .getCodec(), new BufferedInputStream(new LimitInputStream(
                    in, s.getLength())));
            loadDirectoriesInINodeSection(is);
          }
        }

        for (FileSummary.Section s : summary.getSectionsList()) {
          if (SectionName.fromString(s.getName()) == SectionName.INODE_DIR) {
            in.getChannel().position(s.getOffset());
            InputStream is =
                FSImageUtil.wrapInputStreamForCompression(conf, summary
                    .getCodec(), new BufferedInputStream(new LimitInputStream(
                    in, s.getLength())));
            buildNamespace(is);
          }
        }
      }

      for (FileSummary.Section s : summary.getSectionsList()) {
        if (SectionName.fromString(s.getName()) != SectionName.INODE) {
          continue;
        }

        in.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                in, s.getLength())));
        run(is);
        output();
      }
    }
  }

  private void buildNamespace(InputStream in) throws IOException {
    int count = 0;
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      if (e == null) {
        out.println("FsImageProto.INodeDirectorySection.DirEntry is null.");
        break;
      }
      count++;
      if (count % 10000 == 0) {
        out.println("Scanned {} directories: " + count);
      }
      long parentId = e.getParent();
      // Referred INode is not support for now.
      for (int i = 0; i < e.getChildrenCount(); i++) {
        long childId = e.getChildren(i);
        metadataMap.putDirChild(parentId, childId);
      }
      Preconditions.checkState(e.getRefChildrenCount() == 0);
    }
  }

  private void loadDirectoriesInINodeSection(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    out.println("Loading directories in INode section.");
    int numDirs = 0;
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INode p = INode.parseDelimitedFrom(in);
      if (i % 10000 == 0) {
        out.println("Scanned {} inodes: " + i);
      }
      if (p.hasDirectory()) {
        metadataMap.putDir(p);
        numDirs++;
      }
    }
    out.println("Found {} directories in INode section: " + numDirs);
  }

  private void run(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
      if (p.getType() == INodeSection.INode.Type.FILE) {
        ++totalFiles;
        INodeSection.INodeFile f = p.getFile();
        totalBlocks += f.getBlocksCount();
        long fileSize = 0;
        for (BlockProto b : f.getBlocksList()) {
          fileSize += b.getNumBytes();
        }
        maxFileSize = Math.max(fileSize, maxFileSize);
        totalSpace += fileSize * f.getReplication();

        int bucket = fileSize > maxSize ? distribution.length - 1 : (int) Math
            .ceil((double)fileSize / steps);
        ++distribution[bucket];

        if (printSmallFiles && (bucket == 1 || bucket == 0)) {
          increaseSmallFilesCount(prefixStrs, p.getId(), p.getName()
              .toStringUtf8());
        }

      } else if (p.getType() == INodeSection.INode.Type.DIRECTORY) {
        ++totalDirectories;
      }

      if (i % (1 << 20) == 0) {
        out.println("Processed " + i + " inodes.");
      }
    }
  }

  private void output() {
    // write the distribution into the output file
    out.print("Size\tNumFiles\n");
    for (int i = 0; i < distribution.length; i++) {
      if (distribution[i] != 0) {
        out.print("("
            + StringUtils.TraditionalBinaryPrefix.long2String(
                ((long) (i == 0 ? 0 : i - 1) * steps), "", 2)
            + ", "
            + StringUtils.TraditionalBinaryPrefix.long2String(
                ((long) i * steps), "", 2) + "]\t" + distribution[i]);
        out.print('\n');
      }
    }
    out.print("totalFiles = " + totalFiles + "\n");
    out.print("totalDirectories = " + totalDirectories + "\n");
    out.print("totalBlocks = " + totalBlocks + "\n");
    out.print("totalSpace = " + totalSpace + "\n");
    out.print("maxFileSize = " + maxFileSize + "\n");

    if (printSmallFiles) {
      out.print("\n\n");
      out.print("********Small Files List(Total: " + smallFilesMap.size()
          + ")********" + "\n");
      // Print the smallFiles result
      for (Map.Entry<String, Integer> entry : smallFilesMap.entrySet()) {
        out.println("Dir: " + entry.getKey() + ", Count: " + entry.getValue());
      }
    }
  }

  /**
   * Record small file's name
   */
  private void increaseSmallFilesCount(String[] prefixPaths, long nodeId,
      String pathStr) {
    int count = 0;
    String parentPath = "";

    try {
      parentPath = metadataMap.getParentPath(nodeId);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    boolean isMatch = false;
    if (prefixPaths == null || prefixPaths.length == 0) {
      isMatch = true;
    } else {
      for (String str : prefixPaths) {
        if (str != null && str.length() > 0 && parentPath.startsWith(str)) {
          isMatch = true;
          break;
        }
      }
    }

    // Judge if the parentPath match the target prefixPath
    if (!isMatch) {
      return;
    }

    if (!smallFilesMap.containsKey(parentPath)) {
      count = 0;
    } else {
      count = smallFilesMap.get(parentPath);
    }

    count++;
    smallFilesMap.put(parentPath, count);
  }
}
