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
package org.apache.nifi.processors.redis;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import redis.clients.jedis.*;

abstract public class AbstractRedisProcessor extends AbstractProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor
            .Builder()
            .name("hostname")
            .displayName("Hostname")
            .description("Hostname")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder()
            .name("port")
            .displayName("Port")
            .description("Port")
            .required(true)
            .defaultValue("6379")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private AtomicReference<JedisPool> redisPoolRef = new AtomicReference<>();

    protected JedisPool getRedisPool() {
        return redisPoolRef.get();
    }

    protected Jedis getRedisClient() {
        final JedisPool redisPool = getRedisPool();

        Jedis redisClient = null;
        if (redisPool != null) {
            redisClient = redisPool.getResource();
        }

        return redisClient;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String redisHost = context.getProperty(HOSTNAME).getValue();
        final Integer redisPort = context.getProperty(PORT).asInteger();
        final JedisPool redisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
        redisPoolRef.set(redisPool);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        final JedisPool redisPool = redisPoolRef.get();
        if (redisPool != null) {
            redisPool.destroy();
        }
    }
}
