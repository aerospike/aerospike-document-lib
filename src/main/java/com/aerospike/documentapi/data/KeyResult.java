package com.aerospike.documentapi.data;

import com.aerospike.client.Key;
import lombok.Value;

@Value
public class KeyResult {
    Key key;
    Object result;
}
