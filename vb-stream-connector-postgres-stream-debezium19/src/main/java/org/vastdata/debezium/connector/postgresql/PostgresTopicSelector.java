/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.vastdata.debezium.connector.postgresql;

import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;

/**
 * Factory for this connector's {@link TopicSelector}.
 * 复刻自 io.debezium.connector.postgresql.PostgresTopicSelector（debezium-connector-postgres 1.9.7.Final sources，
 * 2026-09-30 复刻），无裁剪、逻辑零改动（2.0 前的 TopicSelector 体系，1.9.7 形态原样）。
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresTopicSelector {

    public static TopicSelector<TableId> create(PostgresConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (id, prefix, delimiter) -> String.join(delimiter, prefix, id.schema(), id.table()));
    }
}
