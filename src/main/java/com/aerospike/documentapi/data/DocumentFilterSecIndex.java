package com.aerospike.documentapi.data;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;

/**
 * Base interface for creating secondary index filter.
 *
 * <p>For the supported json paths see {@link com.aerospike.documentapi.util.FilterConverter}.</p>
 * <p>Supported operators: </p>
 * <ul>
 * <li>EQ</li>
 * <li>GT</li>
 * <li>GE</li>
 * <li>LT</li>
 * <li>LE</li>
 * </ul>
 */
public interface DocumentFilterSecIndex extends DocumentFilter {
    Filter toSecIndexFilter();

    void setIdxCollectionType(IndexCollectionType idxCollectionType);
}
