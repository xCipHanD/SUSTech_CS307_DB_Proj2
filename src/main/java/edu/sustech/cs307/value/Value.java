package edu.sustech.cs307.value;

import java.nio.ByteBuffer;

public class Value {
    public Object value;
    public ValueType type;
    public static final int INT_SIZE = 8;
    public static final int FLOAT_SIZE = 4;
    public static final int DOUBLE_SIZE = 8;
    public static final int CHAR_SIZE = 64;

    public Value(Object value, ValueType type) {
        this.value = value;
        this.type = type;
    }

    public Value(Long value) {
        this.value = value;
        type = ValueType.INTEGER;
    }

    public Value(Float value) {
        this.value = value;
        type = ValueType.FLOAT;
    }

    public Value(Double value) {
        this.value = value;
        type = ValueType.DOUBLE;
    }

    public Value(String value) {
        this.value = value;
        type = ValueType.CHAR;
    }

    /**
     * 将当前值转换为字节数组。
     * 
     * @return 字节数组表示的值，根据值的类型（INTEGER、FLOAT、CHAR）进行转换。
     * @throws RuntimeException 如果值的类型不受支持。
     */
    public byte[] ToByte() {
        return switch (type) {
            case INTEGER -> {
                ByteBuffer buffer1 = ByteBuffer.allocate(INT_SIZE);
                buffer1.putLong((long) value);
                yield buffer1.array();
            }
            case FLOAT -> {
                ByteBuffer buffer2 = ByteBuffer.allocate(FLOAT_SIZE);
                buffer2.putFloat((float) value);
                yield buffer2.array();
            }
            case DOUBLE -> {
                ByteBuffer bufferD = ByteBuffer.allocate(DOUBLE_SIZE);
                bufferD.putDouble((double) value);
                yield bufferD.array();
            }
            case CHAR -> {
                String str = (String) value;
                ByteBuffer buffer3 = ByteBuffer.allocate(CHAR_SIZE);
                buffer3.put(str.getBytes());
                yield buffer3.array();
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        };
    }

    /**
     * 根据给定的字节数组和值类型创建一个 Value 对象。
     *
     * @param bytes 字节数组，表示要转换的值。
     * @param type  值的类型，支持 INTEGER、FLOAT 和 CHAR。
     * @return 转换后的 Value 对象。
     * @throws RuntimeException 如果提供的值类型不受支持。
     */
    public static Value FromByte(byte[] bytes, ValueType type) {
        return switch (type) {
            case INTEGER -> {
                ByteBuffer buffer1 = ByteBuffer.wrap(bytes);
                yield new Value(buffer1.getLong());
            }
            case FLOAT -> {
                ByteBuffer buffer2 = ByteBuffer.wrap(bytes);
                yield new Value(buffer2.getFloat());
            }
            case DOUBLE -> {
                ByteBuffer bufferD = ByteBuffer.wrap(bytes);
                yield new Value(bufferD.getDouble());
            }
            case CHAR -> {
                ByteBuffer buffer3 = ByteBuffer.wrap(bytes);
                String s = new String(bytes);
                yield new Value(s);
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        };

    }

    @Override
    public String toString() {
        switch (type) {
            case INTEGER, FLOAT, DOUBLE -> {
                return this.value.toString();
            }
            case CHAR -> {
                byte[] bytes = ((String) this.value).getBytes();
                String s = new String(bytes);
                s = s.replaceAll("\0", "");
                return s;
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        }
    }

    public boolean isNull() {
        return this.value == null;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Value other))
            return false;
        if (this.type != other.type)
            return false;
        // Compare normalized string representations for CHAR, numeric as value equality
        if (this.value == null && other.value == null)
            return true;
        if (this.value == null || other.value == null)
            return false;
        return this.toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        // Use normalized toString and type to compute hash
        int result = type.hashCode();
        String repr = value == null ? null : toString();
        result = 31 * result + (repr == null ? 0 : repr.hashCode());
        return result;
    }
}
