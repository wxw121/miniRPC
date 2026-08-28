package com.minirpc.core.codec;

import com.minirpc.common.protocol.MessageType;
import com.minirpc.common.protocol.RpcCancellation;
import com.minirpc.common.protocol.RpcMessage;
import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.SerializerType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcCancellationCodecTest {

    @Test
    void roundTripsCancellationFrameUsingVersionTwo() {
        EmbeddedChannel channel = new EmbeddedChannel(new RpcMessageEncoder(), new RpcMessageDecoder());
        RpcMessage cancellation = new RpcMessage(MessageType.CANCEL, SerializerType.HESSIAN2,
                new RpcCancellation("cancel-request-1"));

        assertTrue(channel.writeOutbound(cancellation));
        ByteBuf encoded = channel.readOutbound();
        assertEquals(2, encoded.getUnsignedByte(encoded.readerIndex() + Short.BYTES));

        assertTrue(channel.writeInbound(encoded));
        RpcMessage decoded = channel.readInbound();
        assertEquals(MessageType.CANCEL, decoded.getMessageType());
        assertEquals("cancel-request-1", ((RpcCancellation) decoded.getData()).getRequestId());
        channel.finishAndReleaseAll();
    }

    @Test
    void rejectsCancellationFrameWithNonCancellationBody() {
        EmbeddedChannel channel = new EmbeddedChannel(new RpcMessageEncoder());

        assertThrows(EncoderException.class, () -> channel.writeOutbound(
                new RpcMessage(MessageType.CANCEL, SerializerType.HESSIAN2, new RpcRequest())));
    }

    @Test
    void acceptsVersionOneNormalRequestFrameDuringRollingUpgrade() {
        EmbeddedChannel encoder = new EmbeddedChannel(new RpcMessageEncoder());
        EmbeddedChannel decoder = new EmbeddedChannel(new RpcMessageDecoder());
        RpcRequest request = new RpcRequest();
        request.setRequestId("version-one-request");

        assertTrue(encoder.writeOutbound(new RpcMessage(MessageType.REQUEST, SerializerType.HESSIAN2, request)));
        ByteBuf encoded = encoder.readOutbound();
        encoded.setByte(encoded.readerIndex() + Short.BYTES, 1);

        assertTrue(decoder.writeInbound(encoded));
        RpcMessage decoded = decoder.readInbound();
        assertEquals("version-one-request", ((RpcRequest) decoded.getData()).getRequestId());
        encoder.finishAndReleaseAll();
        decoder.finishAndReleaseAll();
    }
}
