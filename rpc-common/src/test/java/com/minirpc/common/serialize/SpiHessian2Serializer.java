package com.minirpc.common.serialize;

import com.minirpc.common.protocol.SerializerType;

/** A wire-compatible provider discovered from the test service descriptor. */
public class SpiHessian2Serializer extends Hessian2Serializer {

    @Override
    public SerializerType getType() {
        return SerializerType.HESSIAN2;
    }
}
