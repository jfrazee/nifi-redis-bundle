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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.StreamCallback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import redis.clients.jedis.*;

@Tags({"redis"})
@CapabilityDescription("Redis get the number of members in a sorted set")
public class RedisSortedSetSize extends AbstractRedisProcessor {

    public static final PropertyDescriptor KEYS = new PropertyDescriptor
            .Builder()
            .name("keys")
            .displayName("Keys")
            .description("Keys")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(super.getSupportedPropertyDescriptors());
        descriptors.add(KEYS);
        this.descriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<String> keys = Arrays.asList(
            context.getProperty(KEYS)
                .evaluateAttributeExpressions(flowFile)
                .getValue()
                .split("\\s*,\\s*"));

        final Map<String, Long> sizes = new HashMap<>();
        try (Jedis redisClient = getRedisClient()) {
            for (final String key : keys) {
                final Long size = redisClient.zcard(key);
                sizes.put(key, size);
            }
        }
        catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            session.penalize(flowFile);
            context.yield();
        }

        final AtomicBoolean failedRef = new AtomicBoolean(false);
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream is, OutputStream os) throws IOException {
                try (final OutputStream bos = new BufferedOutputStream(os)) {
                    final ObjectMapper mapper = new ObjectMapper();
                    final byte[] bytes = mapper.writeValueAsBytes(sizes);
                    bos.write(bytes);
                    bos.flush();
                }
                catch (Exception e) {
                    getLogger().error(e.getMessage(), e);
                    failedRef.set(true);
                }
            }
        });

        final boolean failed = failedRef.get();
        if (failed) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }
}
