package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.PathDetails;
import com.aerospike.documentapi.util.Lut;
import com.aerospike.documentapi.util.Utils;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.documentapi.util.Utils.createBin;
import static com.aerospike.documentapi.util.Utils.getPathDetails;

class AerospikeDocumentRepository implements IAerospikeDocumentRepository {

    private final IAerospikeClient client;

    AerospikeDocumentRepository(IAerospikeClient client) {
        this.client = client;
    }

    @Override
    public Map<String, Object> get(Policy readPolicy, Key key, Collection<String> binNames,
                                   JsonPathObject jsonPathObject) {
        return get(readPolicy, key, binNames, jsonPathObject, false);
    }

    @Override
    public Map<String, Object> get(Policy readPolicy, Key key, Collection<String> binNames,
                                   JsonPathObject jsonPathObject, boolean withLut) {
        Map<String, Object> results = new HashMap<>();
        // If there are no parts, retrieve the full document
        if (jsonPathObject.getTokensNotRequiringSecondStepQuery().isEmpty()) {
            List<Operation> operations = new ArrayList<>();
            for (String binName : binNames) {
                operations.add(Operation.get(binName));
            }
            if (withLut) {
                operations.add(Lut.LUT_READ_OP);
            }
            WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
            Record rec = client.operate(writePolicy, key, operations.toArray(new Operation[0]));
            if (rec != null) {
                results.putAll(rec.bins);
            }
        } else { // else retrieve using pure contexts
            PathDetails pathDetails = getPathDetails(jsonPathObject.getTokensNotRequiringSecondStepQuery(), true);

            List<Operation> operations = binNames.stream()
                    .map(binName -> pathDetails.getFinalToken().toAerospikeGetOperation(
                            binName,
                            pathDetails.getCtxArray())
                    ).collect(Collectors.toList());

            if (withLut) {
                operations.add(Lut.LUT_READ_OP);
            }

            Record rec;
            try {
                WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
                rec = client.operate(writePolicy, key, operations.toArray(new Operation[0]));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
            if (rec != null) {
                results.putAll(rec.bins);
            }
        }
        return results;
    }

    @Override
    public void put(WritePolicy writePolicy, Key key, String binName, JsonNode jsonNode) {
        client.put(writePolicy, key, Utils.createBinByJsonNodeType(binName, jsonNode));
    }

    @Override
    public void put(WritePolicy writePolicy, Key key, String binName, Map<?, ?> map) {
        client.put(writePolicy, key, new Bin(binName, map));
    }

    @Override
    public void put(WritePolicy writePolicy, Key key, Collection<String> binNames, Object jsonObject,
                    JsonPathObject jsonPathObject) {
        Operation[] operations;
        // If there are no parts, put the full document
        if (jsonPathObject.getTokensNotRequiringSecondStepQuery().isEmpty()) {
            operations = binNames.stream()
                    .map(binName -> {
                        Bin bin = createBin(binName, jsonObject);
                        return Operation.put(bin);
                    })
                    .toArray(Operation[]::new);
            client.operate(writePolicy, key, operations);
        } else { // else put using contexts
            PathDetails pathDetails = getPathDetails(jsonPathObject.getTokensNotRequiringSecondStepQuery(), true);

            try {
                operations = binNames.stream()
                        .map(binName -> pathDetails.getFinalToken().toAerospikePutOperation(
                                binName,
                                jsonObject,
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, key, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void put(WritePolicy writePolicy, Key key, Map<String, Object> queryResults, JsonPathObject jsonPathObject) {
        Operation[] operations;
        // If there are no parts, put the full document
        if (jsonPathObject.getTokensNotRequiringSecondStepQuery().isEmpty()) {
            operations = queryResults.entrySet().stream()
                    .map(e -> {
                        Bin bin = createBin(e.getKey(), e.getValue());
                        return Operation.put(bin);
                    })
                    .toArray(Operation[]::new);
            client.operate(writePolicy, key, operations);
        } else { // else put using contexts
            PathDetails pathDetails = getPathDetails(jsonPathObject.getTokensNotRequiringSecondStepQuery(), true);

            try {
                operations = queryResults.entrySet().stream()
                        .map(entry -> pathDetails.getFinalToken().toAerospikePutOperation(
                                entry.getKey(),
                                entry.getValue(),
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, key, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void append(WritePolicy writePolicy, Key key, Collection<String> binNames, String jsonPath,
                       Object jsonObject, JsonPathObject jsonPathObject) {
        // If there are no parts, you can't append
        if (jsonPathObject.getTokensNotRequiringSecondStepQuery().isEmpty()) {
            throw new DocumentApiException.JsonAppendException(jsonPath);
        } else {
            PathDetails pathDetails = getPathDetails(jsonPathObject.getTokensNotRequiringSecondStepQuery(), false);

            try {
                Operation[] operations = binNames.stream()
                        .map(binName -> pathDetails.getFinalToken().toAerospikeAppendOperation(
                                binName,
                                jsonObject,
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, key, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void delete(WritePolicy writePolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject) {
        // If there are no parts, put an empty map in each given bin
        if (jsonPathObject.getTokensNotRequiringSecondStepQuery().isEmpty()) {
            Operation[] operations = binNames.stream()
                    .map(MapOperation::clear)
                    .toArray(Operation[]::new);
            client.operate(writePolicy, key, operations);
        } else {
            PathDetails pathDetails = getPathDetails(jsonPathObject.getTokensNotRequiringSecondStepQuery(), true);

            try {
                Operation[] operations = binNames.stream()
                        .map(bName -> pathDetails.getFinalToken().toAerospikeDeleteOperation(
                                bName,
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, key, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public boolean batchPerform(BatchPolicy batchPolicy, List<BatchRecord> batchRecords) {
        try {
            return client.operate(batchPolicy, batchRecords);
        } catch (AerospikeException e) {
            throw DocumentApiException.toDocumentException(e);
        }
    }

    @Override
    public RecordSet query(QueryPolicy policy, Statement statement) {
        try {
            return client.query(policy, statement);
        } catch (AerospikeException e) {
            throw DocumentApiException.toDocumentException(e);
        }
    }
}
