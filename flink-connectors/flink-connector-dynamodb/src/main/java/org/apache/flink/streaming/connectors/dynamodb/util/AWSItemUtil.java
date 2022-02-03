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

package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Import the {@code com.amazonaws.services.dynamodbv2.document.ItemUtils} class from AWS sdk v1.
 */
@Internal
public class AWSItemUtil {

    public static Map<String, AttributeValue> toAttributeValueMap(final Map<String, Object> map) {
        if (map == null) {
            return null;
        }
        Map<String, AttributeValue> attributeValueMap = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> e : map.entrySet()) {
            attributeValueMap.put(e.getKey(), toAttributeValue(e.getValue()));
        }
        return attributeValueMap;
    }

    /**
     * Converts a simple value into the low-level {@link AttributeValue} representation.
     *
     * @param value the given value which can be one of the followings:
     *     <ul>
     *       <li>String
     *       <li>Set&lt;String>
     *       <li>Number (including any subtypes and primitive types)
     *       <li>Set&lt;Number>
     *       <li>byte[]
     *       <li>Set&lt;byte[]>
     *       <li>ByteBuffer
     *       <li>Set&lt;ByteBuffer>
     *       <li>Boolean or boolean
     *       <li>null
     *       <li>Map&lt;String,T>, where T can be any type on this list but must not induce any
     *           circular reference
     *       <li>List&lt;T>, where T can be any type on this list but must not induce any circular
     *           reference
     *     </ul>
     *
     * @return a non-null low level representation of the input object value
     * @throws UnsupportedOperationException if the input object type is not supported
     */
    public static AttributeValue toAttributeValue(Object value) {
        AttributeValue.Builder result = AttributeValue.builder();
        if (value == null) {
            return result.nul(Boolean.TRUE).build();
        } else if (value instanceof Boolean) {
            return result.bool((Boolean) value).build();
        } else if (value instanceof String) {
            return result.s((String) value).build();
        } else if (value instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) value;
            return result.n(bd.toPlainString()).build();
        } else if (value instanceof Number) {
            return result.n(value.toString()).build();
        } else if (value instanceof byte[]) {
            return result.b(SdkBytes.fromByteArray(((byte[]) value))).build();
        } else if (value instanceof SdkBytes) {
            return result.b((SdkBytes) value).build();
        } else if (value instanceof Set) {
            // default to an empty string set if there is no element
            @SuppressWarnings("unchecked")
            Set<Object> set = (Set<Object>) value;
            if (set.size() == 0) {
                result.ss(new LinkedHashSet<>());
                return result.build();
            }
            Object element = set.iterator().next();
            if (element instanceof String) {
                @SuppressWarnings("unchecked")
                Set<String> ss = (Set<String>) value;
                result.ss(new ArrayList<>(ss));
            } else if (element instanceof Number) {
                @SuppressWarnings("unchecked")
                Set<Number> in = (Set<Number>) value;
                List<String> out = new ArrayList<>(set.size());
                for (Number n : in) {
                    BigDecimal bd = toBigDecimal(n);
                    out.add(bd.toPlainString());
                }
                result.ns(out);
            } else if (element instanceof byte[]) {
                @SuppressWarnings("unchecked")
                Set<byte[]> in = (Set<byte[]>) value;
                List<SdkBytes> out = new ArrayList<>(set.size());
                for (byte[] buf : in) {
                    out.add(SdkBytes.fromByteArray(buf));
                }
                result.bs(out);
            } else if (element instanceof SdkBytes) {
                @SuppressWarnings("unchecked")
                Set<SdkBytes> bs = (Set<SdkBytes>) value;
                result.bs(bs);
            } else {
                throw new UnsupportedOperationException("element type: " + element.getClass());
            }
        } else if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> in = (List<Object>) value;
            List<AttributeValue> out = new ArrayList<>();
            for (Object v : in) {
                out.add(toAttributeValue(v));
            }
            result.l(out);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> in = (Map<String, Object>) value;
            Map<String, AttributeValue> attributeValueMap = new LinkedHashMap<>(in.size());
            if (in.size() > 0) {
                for (Map.Entry<String, Object> e : in.entrySet()) {
                    attributeValueMap.put(e.getKey(), toAttributeValue(e.getValue()));
                }
                result.m(attributeValueMap);
            } else { // empty map
                result.m(new LinkedHashMap<>());
            }
        } else {
            throw new UnsupportedOperationException("value type: " + value.getClass());
        }
        return result.build();
    }

    /** Converts a number into BigDecimal representation. */
    public static BigDecimal toBigDecimal(Number n) {
        if (n instanceof BigDecimal) {
            return (BigDecimal) n;
        }
        return new BigDecimal(n.toString());
    }
}
