package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;

/**
 * Base interface for creating filter expression.
 *
 * <p>For the supported json paths see {@link com.aerospike.documentapi.util.ExpConverter}.</p>
 * <p>Supported operators: </p>
 * <ul>
 * <li>EQ</li>
 * <li>NE</li>
 * <li>GT</li>
 * <li>GE</li>
 * <li>LT</li>
 * <li>LE</li>
 * <li>REGEX</li>
 * </ul>
 */
public interface DocumentFilterExp extends DocumentFilter {
    Exp toFilterExp();

    void setRegexFlags(int regexFlags);
}
