package org.camunda.tngp.broker.util.msgpack;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.camunda.tngp.broker.test.util.BufferAssert.assertThatBuffer;
import static org.camunda.tngp.broker.util.msgpack.MsgPackUtil.encodeMsgPack;
import static org.camunda.tngp.broker.util.msgpack.MsgPackUtil.utf8;

import java.util.Map;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.broker.util.msgpack.POJO.POJOEnum;
import org.camunda.tngp.msgpack.spec.MsgPackReader;
import org.junit.Test;

public class ObjectMappingDefaultValuesTest
{

    @Test
    public void shouldReturnDefaultValueForMissingProperty()
    {
        // given
        final MutableDirectBuffer msgPackBuffer = encodeMsgPack((w) ->
        {
            w.writeMapHeader(1);
            w.writeString(utf8("noDefaultValueProp"));
            w.writeInteger(123123L);
        });

        final long defaultValue = -1L;
        final DefaultValuesPOJO pojo = new DefaultValuesPOJO(defaultValue);

        // when
        pojo.wrap(msgPackBuffer);

        // then
        assertThat(pojo.getNoDefaultValueProperty()).isEqualTo(123123L);
        assertThat(pojo.getDefaultValueProperty()).isEqualTo(defaultValue);
    }

    @Test
    public void shouldNotReturnDefaultValueForExistingProperty()
    {
        // given
        final MutableDirectBuffer msgPackBuffer = encodeMsgPack((w) ->
        {
            w.writeMapHeader(2);
            w.writeString(utf8("noDefaultValueProp"));
            w.writeInteger(123123L);
            w.writeString(utf8("defaultValueProp"));
            w.writeInteger(987L);
        });

        final long defaultValue = -1L;
        final DefaultValuesPOJO pojo = new DefaultValuesPOJO(defaultValue);

        // when
        pojo.wrap(msgPackBuffer);

        // then
        assertThat(pojo.getNoDefaultValueProperty()).isEqualTo(123123L);
        assertThat(pojo.getDefaultValueProperty()).isEqualTo(987L);
    }

    @Test
    public void shouldReturnDefaultValueAfterReset()
    {
        // given
        final MutableDirectBuffer msgPackBuffer = encodeMsgPack((w) ->
        {
            w.writeMapHeader(2);
            w.writeString(utf8("noDefaultValueProp"));
            w.writeInteger(123123L);
            w.writeString(utf8("defaultValueProp"));
            w.writeInteger(987L);
        });

        final long defaultValue = -1L;
        final DefaultValuesPOJO pojo = new DefaultValuesPOJO(defaultValue);
        pojo.wrap(msgPackBuffer);

        // when
        pojo.reset();

        // then
        assertThat(pojo.getDefaultValueProperty()).isEqualTo(defaultValue);
    }

    /**
     * Default values should be written. Use case: we read a message of version 1 and always
     * write it in version 2, where a new property should be included.
     */
    @Test
    public void shouldWriteDefaultValue()
    {
        // given
        final long defaultValue = -1L;
        final DefaultValuesPOJO pojo = new DefaultValuesPOJO(defaultValue);
        pojo.setNoDefaultValueProperty(123123L);

        final UnsafeBuffer buf = new UnsafeBuffer(new byte[pojo.getLength()]);

        // when
        pojo.write(buf, 0);

        // then
        final MsgPackReader reader = new MsgPackReader();
        reader.wrap(buf, 0, buf.capacity());
        final Map<String, Object> msgPackMap = MsgPackUtil.asMap(buf, 0, buf.capacity());

        assertThat(msgPackMap).hasSize(2);
        assertThat(msgPackMap).contains(
                entry("noDefaultValueProp", 123123L),
                entry("defaultValueProp", defaultValue)
        );
    }

    @Test
    public void shouldSupportDefaultValuesForAllPropertyTypes()
    {
        // given
        final MutableDirectBuffer msgPackBuffer = encodeMsgPack((w) ->
        {
            w.writeMapHeader(0);
        });

        final MutableDirectBuffer packedMsgPackBuffer = encodeMsgPack((w) ->
        {
            w.writeMapHeader(1);
            w.writeInteger(123L);
            w.writeInteger(456L);
        });

        final AllTypesDefaultValuesPOJO pojo = new AllTypesDefaultValuesPOJO(
                POJOEnum.FOO,
                654L,
                123,
                "defaultString",
                packedMsgPackBuffer,
                utf8("defaultBinary")
                );

        // when
        pojo.wrap(msgPackBuffer);

        // then
        assertThat(pojo.getEnum()).isEqualTo(POJOEnum.FOO);
        assertThat(pojo.getLong()).isEqualTo(654L);
        assertThat(pojo.getInt()).isEqualTo(123);
        assertThatBuffer(pojo.getString()).hasBytes(utf8("defaultString"));
        assertThatBuffer(pojo.getPacked()).hasBytes(packedMsgPackBuffer);
        assertThatBuffer(pojo.getBinary()).hasBytes(utf8("defaultBinary"));
    }

}