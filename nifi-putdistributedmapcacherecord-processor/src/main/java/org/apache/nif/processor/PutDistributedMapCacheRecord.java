package org.apache.nif.processor;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutDistributedMapCacheRecord extends AbstractProcessor {
    public static final PropertyDescriptor DISTRIBUTED_MAP_CACHE_CLIENT = new PropertyDescriptor.Builder()
            .name("putdistributedmapcacherecord-dmc-client")
            .displayName("DistributedMapCache Client")
            .required(true)
            .addValidator(Validator.VALID)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }
    }
}
