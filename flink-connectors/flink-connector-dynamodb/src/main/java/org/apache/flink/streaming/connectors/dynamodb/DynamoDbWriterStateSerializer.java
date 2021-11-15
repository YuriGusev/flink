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

package org.apache.flink.streaming.connectors.dynamodb;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

/** A serializer used to serialize a collection of {@link DynamoDbWriteRequest}. */
public class DynamoDbWriterStateSerializer
        implements SimpleVersionedSerializer<Collection<DynamoDbWriteRequest>> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Collection<DynamoDbWriteRequest> obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(obj);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public Collection<DynamoDbWriteRequest> deserialize(int version, byte[] serialized)
            throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final ObjectInputStream in = new ObjectInputStream(bais)) {
            final Collection<DynamoDbWriteRequest> obj =
                    (Collection<DynamoDbWriteRequest>) in.readObject();
            return obj;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
