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
package org.apache.atlas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HdfsUtils {

    /**
     * mkdir on HDFS
     *
     * @param fs hdfs
     * @param path the HDFS path
     * @return if the new dir was made.
     */
    public static boolean mkDir(FileSystem fs, String path) throws IOException {
        return fs.mkdirs(new Path(path));
    }

    /**
     * Get subdir name list
     *
     * @param fs hdfs
     * @param path the path to list sub dirs
     * @param pathPrefix  the prefix to remove
     * @return subdir list(with parent path excluded)
     */
    public static List<String> listDirName(FileSystem fs, Path path, String pathPrefix) throws IOException {
        List<String> dirList = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                dirList.add(fileStatus.getPath().getName().replace(pathPrefix,""));
            }
        }

        return dirList;
    }

    /**
     * Get the sorted subdir name list
     *
     * @param fs hdfs
     * @param path the path to list sub dirs
     * @param pathPrefix  the prefix to remove
     * @return sorted subdir list(with parent path excluded)
     */
    public static List<String> listSortedDirName(FileSystem fs, Path path, String pathPrefix) throws IOException {
        List<String> dirList = listDirName(fs, path, pathPrefix);
        Collections.sort(dirList);

        return dirList;
    }

    /**
     * Delete dir or file
     *
     * @param fs hdfs
     * @param path dir or file
     * @param recursive if the delete strategy is recursive
     * @return if the delete was successful.
     */
    public static boolean deleteFile(FileSystem fs, String path, boolean recursive) throws IOException {
        return fs.delete(new Path(path), recursive);
    }

    public static boolean deleteFile(FileSystem fs, String path) throws IOException {
        return deleteFile(fs, path,true);
    }

    public static void downloadFile(FileSystem fs, String src) throws IOException {
        String dst = "./";
        fs.copyToLocalFile(false, new Path(src), new Path(dst), true);
    }

    /**
     * Get default FileSystem.
     */
    public static FileSystem getDefaultFS(String path) throws IOException {
        return new Path(path).getFileSystem(new Configuration());
    }
}

