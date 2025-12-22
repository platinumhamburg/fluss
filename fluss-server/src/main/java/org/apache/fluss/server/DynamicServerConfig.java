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

package org.apache.fluss.server;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ConfigValidator;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static org.apache.fluss.config.ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * The dynamic configuration for server. If a {@link ServerReconfigurable} implementation class
 * wants to listen for configuration changes, it can register through a method. Subsequently, when
 * {@link DynamicConfigManager} detects changes, it will update the configuration items and push
 * them to these {@link ServerReconfigurable} instances.
 */
@Internal
class DynamicServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicServerConfig.class);
    private static final Set<String> ALLOWED_CONFIG_KEYS =
            new HashSet<>(
                    Arrays.asList(
                            DATALAKE_FORMAT.key(), KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()));
    private static final Set<String> ALLOWED_CONFIG_PREFIXES = Collections.singleton("datalake.");

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Class<? extends ServerReconfigurable>, ServerReconfigurable>
            serverReconfigures = MapUtils.newConcurrentHashMap();

    /** Registered stateless config validators, organized by config key for efficient lookup. */
    private final Map<String, List<ConfigValidator<?>>> configValidatorsByKey =
            MapUtils.newConcurrentHashMap();

    /** The initial configuration items when the server starts from server.yaml. */
    private final Map<String, String> initialConfigMap;

    /** The dynamic configuration items that are added during running(stored in zk). */
    private final Map<String, String> dynamicConfigs = new HashMap<>();

    /**
     * The current configuration map, which is a combination of initial configuration and dynamic.
     */
    private final Map<String, String> currentConfigMap;

    /**
     * The current configuration, which is a combination of initial configuration and dynamic
     * configuration.
     */
    private Configuration currentConfig;

    DynamicServerConfig(Configuration flussConfig) {
        this.currentConfig = flussConfig;
        this.initialConfigMap = flussConfig.toMap();
        this.currentConfigMap = flussConfig.toMap();
    }

    void register(ServerReconfigurable serverReconfigurable) {
        serverReconfigures.put(serverReconfigurable.getClass(), serverReconfigurable);
    }

    /**
     * Register a ConfigValidator for stateless validation.
     *
     * <p>This is typically used by CoordinatorServer to validate configs for components it doesn't
     * run (e.g., KvManager). Validators are stateless and only perform validation without requiring
     * component instances.
     *
     * <p>Validators are organized by config key for efficient lookup. Multiple validators can be
     * registered for the same config key.
     *
     * @param validator the config validator to register
     */
    void registerValidator(ConfigValidator<?> validator) {
        String configKey = validator.configKey();
        configValidatorsByKey
                .computeIfAbsent(configKey, k -> new CopyOnWriteArrayList<>())
                .add(validator);
    }

    /**
     * Update the dynamic configuration and apply to registered ServerReconfigurable. If skipping
     * error config, only the error one will be ignored.
     */
    void updateDynamicConfig(Map<String, String> newDynamicConfigs, boolean skipErrorConfig)
            throws Exception {
        inWriteLock(lock, () -> updateCurrentConfig(newDynamicConfigs, skipErrorConfig));
    }

    Map<String, String> getDynamicConfigs() {
        return inReadLock(lock, () -> new HashMap<>(dynamicConfigs));
    }

    Map<String, String> getInitialServerConfigs() {
        return inReadLock(lock, () -> new HashMap<>(initialConfigMap));
    }

    boolean isAllowedConfig(String key) {
        if (ALLOWED_CONFIG_KEYS.contains(key)) {
            return true;
        }

        for (String prefix : ALLOWED_CONFIG_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private void updateCurrentConfig(Map<String, String> newDynamicConfigs, boolean skipErrorConfig)
            throws Exception {
        // Step 1: Validate all changed configs and collect valid ones
        Map<String, String> validatedChanges = validateConfigs(newDynamicConfigs, skipErrorConfig);

        // No valid changes, return early
        if (validatedChanges.isEmpty()) {
            return;
        }

        // Step 2: Build new configuration with validated changes
        Map<String, String> newProps = new HashMap<>(initialConfigMap);
        overrideProps(newProps, validatedChanges);
        Configuration newConfig = Configuration.fromMap(newProps);

        // No effective change, return early
        if (newProps.equals(currentConfigMap)) {
            return;
        }

        // Step 3: Apply new configuration to all ServerReconfigurable instances
        applyToServerReconfigurables(newConfig, skipErrorConfig);

        // Step 4: Update current state
        currentConfig = newConfig;
        currentConfigMap.clear();
        dynamicConfigs.clear();
        currentConfigMap.putAll(newProps);
        dynamicConfigs.putAll(validatedChanges);
        LOG.info("Dynamic configs changed: {}", validatedChanges);
    }

    /**
     * Validates all config changes and returns only the valid ones.
     *
     * @param newDynamicConfigs configs to validate
     * @param skipErrorConfig whether to skip invalid configs
     * @return validated configs (only changed ones)
     * @throws ConfigException if validation fails and skipErrorConfig is false
     */
    private Map<String, String> validateConfigs(
            Map<String, String> newDynamicConfigs, boolean skipErrorConfig) throws ConfigException {
        Map<String, String> validatedChanges = new HashMap<>();
        Set<String> skippedConfigs = new HashSet<>();

        for (Map.Entry<String, String> entry : newDynamicConfigs.entrySet()) {
            String configKey = entry.getKey();
            String newValueStr = entry.getValue();
            String oldValueStr = currentConfigMap.get(configKey);

            // Skip if value unchanged
            if (Objects.equals(oldValueStr, newValueStr)) {
                continue;
            }

            try {
                validateSingleConfig(configKey, oldValueStr, newValueStr);
                validatedChanges.put(configKey, newValueStr);
            } catch (ConfigException e) {
                LOG.error(
                        "Config validation failed for '{}': {} -> {}. {}",
                        configKey,
                        oldValueStr,
                        newValueStr,
                        e.getMessage());
                if (skipErrorConfig) {
                    skippedConfigs.add(configKey);
                } else {
                    throw e;
                }
            }
        }

        if (!skippedConfigs.isEmpty()) {
            LOG.warn("Skipped invalid configs: {}", skippedConfigs);
        }

        return validatedChanges;
    }

    /**
     * Validates a single config entry including type parsing and business validation.
     *
     * @param configKey config key
     * @param oldValueStr old value string
     * @param newValueStr new value string
     * @throws ConfigException if validation fails
     */
    private void validateSingleConfig(String configKey, String oldValueStr, String newValueStr)
            throws ConfigException {
        // Get ConfigOption for type information
        ConfigOption<?> configOption = ConfigOptions.getConfigOption(configKey);
        if (configOption == null) {
            throw new ConfigException(
                    String.format("No ConfigOption found for config key: %s", configKey));
        }

        // Parse and validate type
        Configuration tempConfig = new Configuration();
        Object newValue = null;
        if (newValueStr != null) {
            tempConfig.setString(configKey, newValueStr);
            try {
                newValue = tempConfig.getOptional(configOption).get();
            } catch (Exception e) {
                throw new ConfigException(
                        String.format(
                                "Cannot parse '%s' as %s for config '%s'",
                                newValueStr,
                                configOption.isList()
                                        ? "List<" + configOption.getClazz().getSimpleName() + ">"
                                        : configOption.getClazz().getSimpleName(),
                                configKey),
                        e);
            }
        }

        // Business validation with registered validators (if any)
        List<ConfigValidator<?>> validators = configValidatorsByKey.get(configKey);
        if (validators != null && !validators.isEmpty()) {
            Object oldValue =
                    oldValueStr != null
                            ? currentConfig.getOptional(configOption).orElse(null)
                            : null;
            for (ConfigValidator<?> validator : validators) {
                invokeValidator(validator, oldValue, newValue);
            }
        }
    }

    /**
     * Applies new configuration to all ServerReconfigurable instances with rollback support.
     *
     * @param newConfig new configuration to apply
     * @param skipErrorConfig whether to skip errors
     * @throws Exception if apply fails and skipErrorConfig is false
     */
    private void applyToServerReconfigurables(Configuration newConfig, boolean skipErrorConfig)
            throws Exception {
        Configuration oldConfig = currentConfig;
        Set<ServerReconfigurable> appliedSet = new HashSet<>();

        // Validate all first
        for (ServerReconfigurable reconfigurable : serverReconfigures.values()) {
            try {
                reconfigurable.validate(newConfig);
            } catch (ConfigException e) {
                LOG.error(
                        "Validation failed for {}: {}",
                        reconfigurable.getClass().getSimpleName(),
                        e.getMessage(),
                        e);
                if (!skipErrorConfig) {
                    throw e;
                }
            }
        }

        // Apply to all instances
        Exception throwable = null;
        for (ServerReconfigurable reconfigurable : serverReconfigures.values()) {
            try {
                reconfigurable.reconfigure(newConfig);
                appliedSet.add(reconfigurable);
            } catch (ConfigException e) {
                LOG.error(
                        "Reconfiguration failed for {}: {}",
                        reconfigurable.getClass().getSimpleName(),
                        e.getMessage(),
                        e);
                if (!skipErrorConfig) {
                    throwable = e;
                    break;
                }
            }
        }

        // Rollback if there was an error
        if (throwable != null) {
            appliedSet.forEach(r -> r.reconfigure(oldConfig));
            throw throwable;
        }
    }

    private void overrideProps(Map<String, String> props, Map<String, String> propsOverride) {
        propsOverride.forEach(
                (key, value) -> {
                    if (value == null) {
                        props.remove(key);
                    } else {
                        props.put(key, value);
                    }
                });
    }

    /**
     * Invokes validator with strongly-typed values.
     *
     * @param validator the config validator to invoke
     * @param oldValue the old typed value
     * @param newValue the new typed value
     * @throws ConfigException if validation fails
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void invokeValidator(ConfigValidator<?> validator, Object oldValue, Object newValue)
            throws ConfigException {
        // Invoke validator with typed values
        // We suppress unchecked warning because we trust that the validator
        // is registered for the correct type matching the ConfigOption
        ((ConfigValidator) validator).validate(oldValue, newValue);
    }
}
