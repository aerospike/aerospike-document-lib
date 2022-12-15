package com.aerospike.documentapi.jsonpath;

import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.jsonpath.pathpart.PathPart;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class PathDetails {
    PathPart finalPathPart;
    CTX[] ctxArray;
}