package org.apache.nifi.processor;

import org.apache.nif.processor.PutDistributedMapCacheRecord;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPutDistributedMapCacheRecord {
    private TestRunner runner;
    DistributedMapCacheClient client;

    @BeforeEach
    public void setup() throws Exception {
        client = new MockCacheClient();
        runner = TestRunners.newTestRunner(PutDistributedMapCacheRecord.class);
        MockRecordParser recordReader = new MockRecordParser();
        recordReader.addSchemaField("key", RecordFieldType.STRING);
        recordReader.addSchemaField("value", RecordFieldType.LONG);
        recordReader.addRecord("a", 1l);
        recordReader.addRecord("b", 2l);
        recordReader.addRecord("c", 3l);
        recordReader.addRecord("d", 4l);
        runner.addControllerService("reader", recordReader);
        runner.enableControllerService(recordReader);
        runner.addControllerService("dmc", client);
        runner.enableControllerService(client);
        runner.setProperty(PutDistributedMapCacheRecord.RECORD_READER, "reader");
        runner.setProperty(PutDistributedMapCacheRecord.DISTRIBUTED_MAP_CACHE_CLIENT, "dmc");
        runner.setProperty(PutDistributedMapCacheRecord.CACHE_ENTRY_IDENTIFIER_RP, "/key");
        runner.setProperty(PutDistributedMapCacheRecord.CACHE_ENTRY_VALUE_RP, "/value");
        runner.assertValid();
    }

    @Test
    public void test() {
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(PutDistributedMapCacheRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutDistributedMapCacheRecord.REL_SUCCESS, 1);
        assertEquals(4, ((MockCacheClient)client).getPutCalled());
    }

    static class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private int putCalled = 0;

        public int getPutCalled() {
            return putCalled;
        }

        @Override
        public <K, V> boolean putIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {
            return false;
        }

        @Override
        public <K, V> V getAndPutIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1, Deserializer<V> deserializer) throws IOException {
            getLogger().info("Called getAndPutIfAbsent");
            return null;
        }

        @Override
        public <K> boolean containsKey(K k, Serializer<K> serializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> void put(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {
            getLogger().info("Called put");
            putCalled++;
        }

        @Override
        public <K, V> void putAll(Map<K, V> keysAndValues, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            DistributedMapCacheClient.super.putAll(keysAndValues, keySerializer, valueSerializer);
        }

        @Override
        public <K, V> V get(K k, Serializer<K> serializer, Deserializer<V> deserializer) throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public <K> boolean remove(K k, Serializer<K> serializer) throws IOException {
            return false;
        }

        @Override
        public long removeByPattern(String s) throws IOException {
            return 0;
        }
    }
}
