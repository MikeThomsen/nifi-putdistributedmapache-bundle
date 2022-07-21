/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nif.processor;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutDistributedMapCacheRecord extends AbstractProcessor {
    public static final PropertyDescriptor DISTRIBUTED_MAP_CACHE_CLIENT = new PropertyDescriptor.Builder()
            .name("putdistributedmapcacherecord-dmc-client")
            .displayName("DistributedMapCache Client")
            .required(true)
            .addValidator(Validator.VALID)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will " +
                    "be evaluated against a FlowFile in order to determine the cache key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue CACHE_UPDATE_REPLACE = new AllowableValue("replace", "Replace if present",
            "Adds the specified entry to the cache, replacing any value that is currently set.");

    public static final AllowableValue CACHE_UPDATE_KEEP_ORIGINAL = new AllowableValue("keeporiginal", "Keep original",
            "Adds the specified entry to the cache, if the key does not exist.");

    public static final PropertyDescriptor CACHE_UPDATE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Cache update strategy")
            .description("Determines how the cache is updated if the cache already contains the entry")
            .required(true)
            .allowableValues(CACHE_UPDATE_REPLACE, CACHE_UPDATE_KEEP_ORIGINAL)
            .defaultValue(CACHE_UPDATE_REPLACE.getValue())
            .build();

    public static final PropertyDescriptor CACHE_ENTRY_MAX_BYTES = new PropertyDescriptor.Builder()
            .name("Max cache entry size")
            .description("The maximum amount of data to put into cache")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully inserted into cache will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be inserted into the cache will be routed to this relationship")
            .build();
    private final Set<Relationship> relationships;

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Serializer<byte[]> valueSerializer = new CacheValueSerializer();
    private final Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();

    public PutDistributedMapCacheRecord() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CACHE_ENTRY_IDENTIFIER);
        descriptors.add(DISTRIBUTED_MAP_CACHE_CLIENT);
        descriptors.add(CACHE_UPDATE_STRATEGY);
        descriptors.add(CACHE_ENTRY_MAX_BYTES);
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }
    }

    public static class CacheValueSerializer implements Serializer<byte[]> {

        @Override
        public void serialize(final byte[] bytes, final OutputStream out) throws SerializationException, IOException {
            out.write(bytes);
        }
    }

    public static class CacheValueDeserializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }
    }

    /**
     * Simple string serializer, used for serializing the cache key
     */
    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
