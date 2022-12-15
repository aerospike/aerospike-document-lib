package com.aerospike.documentapi.util;

import com.aerospike.client.Operation;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Lut {

    public static final String LUT_BIN = "__lut_" + Lut.class.hashCode();

    public static final Operation LUT_READ_OP =
            ExpOperation.read(LUT_BIN, Exp.build(Exp.lastUpdate()), ExpReadFlags.DEFAULT);

    public static WritePolicy setLutPolicy(WritePolicy writePolicy, long lut) {
        writePolicy.filterExp = Exp.build(
                Exp.eq(
                        Exp.lastUpdate(),
                        Exp.val(lut)
                )
        );
        writePolicy.failOnFilteredOut = true;
        return writePolicy;
    }

    public static BatchWritePolicy setLutPolicy(BatchWritePolicy batchWritePolicy, long lut) {
        batchWritePolicy.filterExp = Exp.build(
                Exp.eq(
                        Exp.lastUpdate(),
                        Exp.val(lut)
                )
        );
        return batchWritePolicy;
    }
}
