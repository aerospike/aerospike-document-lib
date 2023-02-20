package com.aerospike.documentapi.policy;

import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DocumentPolicy {

    Policy readPolicy;
    WritePolicy writePolicy;
    BatchPolicy batchPolicy;
    QueryPolicy queryPolicy;
}
