/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.vastdata.debezium.connector.postgresql.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * A semantic type for a Ltree string.
 * 复刻自 io.debezium.connector.postgresql.data.Ltree（debezium-connector-postgres 1.9.7.Final sources，2026-09-30 裁剪复刻）
 *
 * @author Mincong Huang
 */
public class Ltree {

    public static final String LOGICAL_NAME = "io.debezium.data.Ltree";

    /**
     * Returns a {@link SchemaBuilder} for a Ltree field. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     *
     * @return the schema builder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME)
                .version(1);
    }

    /**
     * Returns a {@link SchemaBuilder} for a Ltree field, with all other default Schema settings.
     *
     * @return the schema
     * @see #builder()
     */
    public static Schema schema() {
        return builder().build();
    }
}
