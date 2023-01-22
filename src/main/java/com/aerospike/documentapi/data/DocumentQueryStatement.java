package com.aerospike.documentapi.data;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DocumentQueryStatement {
    String namespace;
    String setName;
    String indexName;
    String[] binNames;
    long maxRecords;
    int recordsPerSecond;
    String[] jsonPaths;
    DocumentFilter documentFilter;

    public Statement toStatement() {
        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);
        statement.setIndexName(indexName);
        statement.setBinNames(binNames);
        statement.setMaxRecords(maxRecords);
        statement.setRecordsPerSecond(recordsPerSecond);
        statement.setFilter(documentFilter == null ? null : documentFilter.toSecIndexFilter());
        return statement;
    }
}
