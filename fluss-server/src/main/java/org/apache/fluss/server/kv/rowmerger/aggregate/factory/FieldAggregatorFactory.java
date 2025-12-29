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

package org.apache.fluss.server.kv.rowmerger.aggregate.factory;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;
import org.apache.fluss.types.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Factory interface for creating {@link FieldAggregator} instances. */
public interface FieldAggregatorFactory {

    /**
     * Creates a field aggregator with parameters from AggFunction.
     *
     * @param fieldType the data type of the field
     * @param aggFunction the aggregation function with parameters
     * @param field the field name
     * @return the field aggregator
     */
    FieldAggregator create(DataType fieldType, AggFunction aggFunction, String field);

    /**
     * Returns the unique identifier for this factory.
     *
     * @return the identifier string
     */
    String identifier();

    /**
     * Creates a field aggregator for the given field with parameterized aggregate function.
     *
     * @param fieldType the data type of the field
     * @param fieldName the field name
     * @param aggFunction the aggregation function with parameters
     * @return the field aggregator
     */
    static FieldAggregator create(DataType fieldType, String fieldName, AggFunction aggFunction) {
        String identifier = aggFunction.getType().getIdentifier();
        FieldAggregatorFactory factory = getFactory(identifier);
        if (factory == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported aggregation function: %s or spell aggregate function incorrectly!",
                            identifier));
        }

        return factory.create(fieldType, aggFunction, fieldName);
    }

    /**
     * Gets a factory by its identifier using SPI.
     *
     * @param identifier the identifier string
     * @return the factory, or null if not found
     */
    static FieldAggregatorFactory getFactory(String identifier) {
        return FactoryRegistry.INSTANCE.getFactory(identifier);
    }

    /** Registry for field aggregator factories using Java SPI. */
    class FactoryRegistry {
        private static final FactoryRegistry INSTANCE = new FactoryRegistry();
        private final Map<String, FieldAggregatorFactory> factories;

        private FactoryRegistry() {
            this.factories = new HashMap<>();
            loadFactories();
        }

        private void loadFactories() {
            ServiceLoader<FieldAggregatorFactory> loader =
                    ServiceLoader.load(
                            FieldAggregatorFactory.class,
                            FieldAggregatorFactory.class.getClassLoader());
            for (FieldAggregatorFactory factory : loader) {
                factories.put(factory.identifier(), factory);
            }
        }

        FieldAggregatorFactory getFactory(String identifier) {
            return factories.get(identifier);
        }
    }
}
