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

package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KuduSplit implements ConnectorSplit {

    private final KuduTableHandle tableHandle;
    private final int primaryKeyColumnCount;
    private final byte[] pb;

    @JsonCreator
    public KuduSplit(@JsonProperty("tableHandle") KuduTableHandle tableHandle,
                     @JsonProperty("primaryKeyColumnCount") int primaryKeyColumnCount,
                     @JsonProperty("pb") byte[] pb) {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.primaryKeyColumnCount = primaryKeyColumnCount;
        this.pb = requireNonNull(pb, "pb is null");
    }

    @JsonProperty
    public KuduTableHandle getTableHandle() { return tableHandle; }

    @JsonProperty
    public byte[] getPb() {
        return pb;
    }

    @JsonProperty
    public int getPrimaryKeyColumnCount() {
        return primaryKeyColumnCount;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}

