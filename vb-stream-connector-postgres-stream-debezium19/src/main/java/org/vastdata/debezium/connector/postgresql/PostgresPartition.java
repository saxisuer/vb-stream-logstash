/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * 流分区：PG 单分区连接器，分区键恰一个 {@code server}（逻辑名）——equals/hashCode 仅按 serverName
 * 相等。复刻自 io.debezium.connector.postgresql.PostgresPartition（debezium-connector-postgres
 * 1.9.7.Final sources，2026-09-30 复刻），无裁剪、逻辑零改动；嵌套 {@code Provider} 为包私有类，
 * vanilla 由 {@code PostgresStreamingChangeEventSource}（排除集）构造——本连接器接缝以等价 lambda
 * 提供分区（{@code .stream/PostgresStreamConnectorTask}），类体原样保留。
 */
public class PostgresPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public PostgresPartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final PostgresPartition other = (PostgresPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "PostgresPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<PostgresPartition> {
        private final PostgresConnectorConfig connectorConfig;

        Provider(PostgresConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<PostgresPartition> getPartitions() {
            return Collections.singleton(new PostgresPartition(connectorConfig.getLogicalName()));
        }
    }
}
