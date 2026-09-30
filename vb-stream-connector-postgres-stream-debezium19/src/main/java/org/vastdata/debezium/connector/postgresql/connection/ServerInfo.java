/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.vastdata.debezium.connector.postgresql.connection;

/**
 * Information about a running Postgres instance.
 * 复刻自 io.debezium.connector.postgresql.connection.ServerInfo（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻）。
 * 裁剪说明：仅保留嵌套枚举 {@link ReplicaIdentity}——自含化保留集中 {@code PostgresConnection.readReplicaIdentityInfo}
 * 的唯一使用面；类壳的 server/username/database/permissions 探测字段与 {@code ReplicationSlot} 嵌套类
 * （pg configuration/槽状态探测面，排除集）连同其 {@code SlotState} 依赖一并删除。
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class ServerInfo {

    /**
     * Table REPLICA IDENTITY information.
     */
    public enum ReplicaIdentity {
        NOTHING("UPDATE and DELETE events will not contain any old values"),
        FULL("UPDATE AND DELETE events will contain the previous values of all the columns"),
        DEFAULT("UPDATE and DELETE events will contain previous values only for PK columns"),
        INDEX("UPDATE and DELETE events will contain previous values only for columns present in the REPLICA IDENTITY index"),
        UNKNOWN("Unknown REPLICA IDENTITY");

        private String description;

        /**
         * Returns a textual description of the replica identity
         *
         * @return a description, never null
         */
        public String description() {
            return this.description;
        }

        ReplicaIdentity(String description) {
            this.description = description;
        }

        protected static ReplicaIdentity parseFromDB(String s) {
            switch (s) {
                case "n":
                    return NOTHING;
                case "d":
                    return DEFAULT;
                case "i":
                    return INDEX;
                case "f":
                    return FULL;
                default:
                    return UNKNOWN;
            }
        }

    }
}
