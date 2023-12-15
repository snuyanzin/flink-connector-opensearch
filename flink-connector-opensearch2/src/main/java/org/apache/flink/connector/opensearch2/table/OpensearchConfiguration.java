/*
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

package org.apache.flink.connector.opensearch2.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch2.sink.FlushBackoffType;
import org.apache.flink.table.api.ValidationException;

import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Opensearch base configuration. */
@Internal
class OpensearchConfiguration {
    protected final ReadableConfig config;

    OpensearchConfiguration(ReadableConfig config) {
        this.config = checkNotNull(config);
    }

    public int getBulkFlushMaxActions() {
        return config.get(OpensearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION);
    }

    public MemorySize getBulkFlushMaxByteSize() {
        return config.get(OpensearchConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION);
    }

    public long getBulkFlushInterval() {
        return config.get(OpensearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION).toMillis();
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return config.get(OpensearchConnectorOptions.DELIVERY_GUARANTEE_OPTION);
    }

    public Optional<String> getUsername() {
        return config.getOptional(OpensearchConnectorOptions.USERNAME_OPTION);
    }

    public Optional<String> getPassword() {
        return config.getOptional(OpensearchConnectorOptions.PASSWORD_OPTION);
    }

    public Optional<FlushBackoffType> getBulkFlushBackoffType() {
        return config.getOptional(OpensearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION);
    }

    public Optional<Integer> getBulkFlushBackoffRetries() {
        return config.getOptional(OpensearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION);
    }

    public Optional<Long> getBulkFlushBackoffDelay() {
        return config.getOptional(OpensearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION)
                .map(Duration::toMillis);
    }

    public String getIndex() {
        return config.get(OpensearchConnectorOptions.INDEX_OPTION);
    }

    public String getKeyDelimiter() {
        return config.get(OpensearchConnectorOptions.KEY_DELIMITER_OPTION);
    }

    public Optional<String> getPathPrefix() {
        return config.getOptional(OpensearchConnectorOptions.CONNECTION_PATH_PREFIX_OPTION);
    }

    public Optional<Duration> getConnectionRequestTimeout() {
        return config.getOptional(OpensearchConnectorOptions.CONNECTION_REQUEST_TIMEOUT);
    }

    public Optional<Duration> getConnectionTimeout() {
        return config.getOptional(OpensearchConnectorOptions.CONNECTION_TIMEOUT);
    }

    public Optional<Duration> getSocketTimeout() {
        return config.getOptional(OpensearchConnectorOptions.SOCKET_TIMEOUT);
    }

    public List<HttpHost> getHosts() {
        return config.get(OpensearchConnectorOptions.HOSTS_OPTION).stream()
                .map(OpensearchConfiguration::validateAndParseHostsString)
                .collect(Collectors.toList());
    }

    public Optional<Integer> getParallelism() {
        return config.getOptional(SINK_PARALLELISM);
    }

    public Optional<Boolean> isAllowInsecure() {
        return config.getOptional(OpensearchConnectorOptions.ALLOW_INSECURE);
    }

    private static HttpHost validateAndParseHostsString(String host) {
        try {
            HttpHost httpHost = HttpHost.create(host);
            if (httpHost.getPort() < 0) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
                                host, OpensearchConnectorOptions.HOSTS_OPTION.key()));
            }

            if (httpHost.getSchemeName() == null) {
                throw new ValidationException(
                        String.format(
                                "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
                                host, OpensearchConnectorOptions.HOSTS_OPTION.key()));
            }
            return httpHost;
        } catch (Exception e) {
            throw new ValidationException(
                    String.format(
                            "Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
                            host, OpensearchConnectorOptions.HOSTS_OPTION.key()),
                    e);
        }
    }
}
