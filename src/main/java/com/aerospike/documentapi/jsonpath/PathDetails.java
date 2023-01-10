package com.aerospike.documentapi.jsonpath;

import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.token.ContextAwareToken;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class PathDetails {
    ContextAwareToken finalToken;
    CTX[] ctxArray;
}