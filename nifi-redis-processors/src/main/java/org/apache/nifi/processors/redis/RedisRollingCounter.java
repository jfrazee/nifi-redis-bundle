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
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import redis.clients.jedis.*;

@Tags({"redis"})
@CapabilityDescription("Redis keep a rolling counter over a specified time interval")
public class RedisRollingCounter extends AbstractRedisProcessor {

    public static final PropertyDescriptor KEY = new PropertyDescriptor
            .Builder()
            .name("key")
            .displayName("Key")
            .description("Key")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIME_INTERVAL = new PropertyDescriptor
            .Builder()
            .name("time-interval")
            .displayName("Interval")
            .description("Interval")
            .expressionLanguageSupported(true)
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(super.getSupportedPropertyDescriptors());
        descriptors.add(KEY);
        descriptors.add(TIME_INTERVAL);
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

        final String key = context.getProperty(KEY)
             .evaluateAttributeExpressions(flowFile)
             .getValue();

        final int interval = context.getProperty(TIME_INTERVAL)
             .evaluateAttributeExpressions(flowFile)
             .asTimePeriod(TimeUnit.SECONDS)
             .intValue();

        final String uuid = flowFile.getAttribute(CoreAttributes.UUID.key());
        final long lineageStartDate = flowFile.getLineageStartDate();

        try (Jedis redisClient = getRedisClient()) {
            final long currentTime = System.currentTimeMillis();
            final long endTime = currentTime - (interval * 1000);

            final Transaction t = redisClient.multi();
            t.zremrangeByScore(key, 0, endTime);
            t.zadd(key, lineageStartDate, uuid);
            t.expire(key, interval);
            t.exec();
        }
        catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            session.penalize(flowFile);
            context.yield();
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
