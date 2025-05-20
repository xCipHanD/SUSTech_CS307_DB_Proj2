package value;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValueComparerTest {

    @Test
    @DisplayName("整数比较1")
    void testCompareIntegerEqual() throws DBException {
        Value v1 = new Value(10L);
        Value v2 = new Value(10L);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(0); // 10 == 10
    }

    @Test
    @DisplayName("整数比较2")
    void testCompareIntegerGreaterThan() throws DBException {
        Value v1 = new Value(15L);
        Value v2 = new Value(10L);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(1); // 15 > 10
    }

    @Test
    @DisplayName("整数比较3")
    void testCompareIntegerLessThan() throws DBException {
        Value v1 = new Value(5L);
        Value v2 = new Value(10L);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(-1); // 5 < 10
    }

    @Test
    @DisplayName("浮点数比较1")
    void testCompareFloatEqual() throws DBException {
        Value v1 = new Value(10.5f); // Use f suffix for float
        Value v2 = new Value(10.5f); // Use f suffix for float

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(0); // 10.5f == 10.5f
    }

    @Test
    @DisplayName("浮点数比较2")
    void testCompareFloatGreaterThan() throws DBException {
        Value v1 = new Value(15.5f); // Use f suffix for float
        Value v2 = new Value(10.5f); // Use f suffix for float

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(1); // 15.5f > 10.5f
    }

    @Test
    @DisplayName("浮点数比较3")
    void testCompareFloatLessThan() throws DBException {
        Value v1 = new Value(5.5f); // Use f suffix for float
        Value v2 = new Value(10.5f); // Use f suffix for float

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(-1); // 5.5f < 10.5f
    }

    @Test
    @DisplayName("双精度浮点数比较1")
    void testCompareDoubleEqual() throws DBException {
        Value v1 = new Value(10.55); // Default to double if not representable as float or explicitly Double
        Value v2 = new Value(10.55);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(0); // 10.55 == 10.55
    }

    @Test
    @DisplayName("双精度浮点数比较2")
    void testCompareDoubleGreaterThan() throws DBException {
        Value v1 = new Value(15.55);
        Value v2 = new Value(10.55);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(1); // 15.55 > 10.55
    }

    @Test
    @DisplayName("双精度浮点数比较3")
    void testCompareDoubleLessThan() throws DBException {
        Value v1 = new Value(5.55);
        Value v2 = new Value(10.55);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(-1); // 5.55 < 10.55
    }

    @Test
    @DisplayName("字符串比较1")
    void testCompareStringEqual() throws DBException {
        Value v1 = new Value("apple");
        Value v2 = new Value("apple");

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(0); // "apple" == "apple"
    }

    @Test
    @DisplayName("字符串比较2")
    void testCompareStringGreaterThan() throws DBException {
        Value v1 = new Value("banana");
        Value v2 = new Value("apple");

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(1); // "banana" > "apple"
    }

    @Test
    @DisplayName("字符串比较3")
    void testCompareStringLessThan() throws DBException {
        Value v1 = new Value("apple");
        Value v2 = new Value("banana");

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(-1); // "apple" < "banana"
    }

    @Test
    @DisplayName("V1为Null")
    void testCompareNullV1() throws DBException {
        Value v1 = null;
        Value v2 = new Value(10L);

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(-1); // v1 is null, so return -1
    }

    @Test
    @DisplayName("V2为Null")
    void testCompareNullV2() throws DBException {
        Value v1 = new Value(10L);
        Value v2 = null;

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(1); // v2 is null, so return 1
    }

    @Test
    @DisplayName("两者同时为null")
    void testCompareNullBoth() throws DBException {
        Value v1 = null;
        Value v2 = null;

        int result = ValueComparer.compare(v1, v2);

        assertThat(result).isEqualTo(-1); // Both null, return -1
    }

    @Test
    @DisplayName("不同类型比较")
    void testCompareDifferentTypes() {
        Value v1 = new Value(10L);
        Value v2 = new Value(10.0f); // Use f suffix for float

        assertThatThrownBy(() -> ValueComparer.compare(v1, v2))
                .isInstanceOf(DBException.class)
                .hasMessageContaining("WRONG_COMPARISON_TYPE");
    }

    @Test
    @DisplayName("测试使用Object和ValueType构造函数")
    public void testConstructorWithObjectAndType() {
        Value value = new Value(123L, ValueType.INTEGER);
        assertThat(value.value).isEqualTo(123L);
        assertThat(value.type).isEqualTo(ValueType.INTEGER);

        value = new Value(123.45f, ValueType.FLOAT); // Use f suffix for float
        assertThat(value.value).isEqualTo(123.45f);
        assertThat(value.type).isEqualTo(ValueType.FLOAT);

        value = new Value(123.4567, ValueType.DOUBLE); // Explicitly Double
        assertThat(value.value).isEqualTo(123.4567);
        assertThat(value.type).isEqualTo(ValueType.DOUBLE);

        value = new Value("test", ValueType.CHAR);
        assertThat(value.value).isEqualTo("test");
        assertThat(value.type).isEqualTo(ValueType.CHAR);
    }

    @Test
    @DisplayName("测试使用Long构造函数")
    public void testConstructorWithLong() {
        Value value = new Value(123L);
        assertThat(value.value).isEqualTo(123L);
        assertThat(value.type).isEqualTo(ValueType.INTEGER);
    }

    @Test
    @DisplayName("测试使用Double构造函数")
    public void testConstructorWithDouble() {
        // Test case 1: Value that can be a float
        Value valueFloat = new Value(123.5); // Will be treated as Float if it fits
        assertThat(valueFloat.value).isEqualTo(123.5d);
        assertThat(valueFloat.type).isEqualTo(ValueType.DOUBLE);

        // Test case 2: Value that is explicitly a float
        Value valueExplicitFloat = new Value(123.5f);
        assertThat(valueExplicitFloat.value).isEqualTo(123.5);
        assertThat(valueExplicitFloat.type).isEqualTo(ValueType.FLOAT);

        // Test case 3: Value that must be a double (e.g., higher precision)
        Value valueDouble = new Value(123.4567890123);
        assertThat(valueDouble.value).isEqualTo(123.4567890123);
        assertThat(valueDouble.type).isEqualTo(ValueType.DOUBLE);

        // Test case 4: Value explicitly typed as Double
        Value valueExplicitDouble = new Value(123.5, ValueType.DOUBLE);
        assertThat(valueExplicitDouble.value).isEqualTo(123.5);
        assertThat(valueExplicitDouble.type).isEqualTo(ValueType.DOUBLE);
    }

    @Test
    @DisplayName("测试使用String构造函数")
    public void testConstructorWithString() {
        Value value = new Value("test");
        assertThat(value.value).isEqualTo("test");
        assertThat(value.type).isEqualTo(ValueType.CHAR);
    }

    @ParameterizedTest
    @MethodSource("provideToByteData")
    @DisplayName("测试ToByte方法")
    public void testToByte(Object value, ValueType type, byte[] expectedBytes) {
        Value valueObj = new Value(value, type);
        assertThat(valueObj.ToByte()).isEqualTo(expectedBytes);
    }

    private static Stream<Arguments> provideToByteData() {
        return Stream.of(
                Arguments.of(123L, ValueType.INTEGER, ByteBuffer.allocate(8).putLong(123L).array()),
                Arguments.of(123.45, ValueType.FLOAT, ByteBuffer.allocate(8).putDouble(123.45).array()), // Updated for
                                                                                                          // Float
                Arguments.of(123.456789, ValueType.DOUBLE, ByteBuffer.allocate(8).putDouble(123.456789).array()),
                Arguments.of("test", ValueType.CHAR,ByteBuffer.allocate(64).put("test".getBytes()).array())); // Corrected
                                                                                                          // CHAR
                                                                                                          // serialization
    }

    @ParameterizedTest
    @MethodSource("provideFromByteData")
    @DisplayName("测试FromByte方法")
    public void testFromByte(byte[] bytes, ValueType type, Object expectedValue) {
        Value valueObj = Value.FromByte(bytes, type);
        assertThat(valueObj.value).isEqualTo(expectedValue);
        assertThat(valueObj.type).isEqualTo(type);
    }

    private static Stream<Arguments> provideFromByteData() {
        return Stream.of(
                // Arguments.of(ByteBuffer.allocate(8).putLong(123L).array(), ValueType.INTEGER, 123L),
                Arguments.of(ByteBuffer.allocate(8).putDouble(123.45).array(), ValueType.FLOAT, 123.45), // Updated for
                                                                                                          // Float
                Arguments.of(ByteBuffer.allocate(8).putDouble(123.456789).array(), ValueType.DOUBLE, 123.456789),
                Arguments.of(ByteBuffer.allocate(64).putInt("test".length()).put("test".getBytes()).array(),
                        ValueType.CHAR, "test")); // Corrected CHAR deserialization
    }

    @Test
    @DisplayName("测试ToByte方法在不支持的类型时抛出异常")
    public void testToByteWithUnsupportedType() {
        Value value = new Value(123L, ValueType.UNKNOWN);
        assertThrows(RuntimeException.class, value::ToByte);
    }

    @Test
    @DisplayName("测试FromByte方法在不支持的类型时抛出异常")
    public void testFromByteWithUnsupportedType() {
        byte[] bytes = ByteBuffer.allocate(8).putLong(123L).array();
        assertThrows(RuntimeException.class, () -> Value.FromByte(bytes, ValueType.UNKNOWN));
    }
}