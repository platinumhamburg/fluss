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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class to uniquely identify a combination of data table bucket and index table bucket for index
 * replication purposes.
 */
@Internal
public class DataIndexTableBucket implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableBucket dataBucket;
    private final TableBucket indexBucket;

    public DataIndexTableBucket(TableBucket dataBucket, TableBucket indexBucket) {
        this.dataBucket = Objects.requireNonNull(dataBucket, "dataBucket cannot be null");
        this.indexBucket = Objects.requireNonNull(indexBucket, "indexBucket cannot be null");
    }

    public TableBucket getDataBucket() {
        return dataBucket;
    }

    public TableBucket getIndexBucket() {
        return indexBucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataIndexTableBucket that = (DataIndexTableBucket) o;
        return Objects.equals(dataBucket, that.dataBucket)
                && Objects.equals(indexBucket, that.indexBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataBucket, indexBucket);
    }

    @Override
    public String toString() {
        return "DataIndexTableBucket{"
                + "dataBucket="
                + dataBucket
                + ", indexBucket="
                + indexBucket
                + '}';
    }
}
