package getl.utils

import getl.test.GetlTest
import org.junit.Test

class NumericUtilsTest extends GetlTest {
    @Test
    void testObj2Integer() {
        def res = 100
        assertNull(NumericUtils.Obj2Integer(null))
        assertNull(NumericUtils.Obj2Integer(''))
        assertEquals(res, NumericUtils.Obj2Integer('100'))
        assertEquals(res, NumericUtils.Obj2Integer(100))
        assertEquals(res, NumericUtils.Obj2Integer(100L))
        assertEquals(res, NumericUtils.Obj2Integer(BigDecimal.valueOf(100L)))
        assertEquals(res, NumericUtils.Obj2Integer(BigInteger.valueOf(100L)))
    }

    @Test
    void testObj2Long() {
        def res = 100L
        assertNull(NumericUtils.Obj2Long(null))
        assertNull(NumericUtils.Obj2Long(''))
        assertEquals(res, NumericUtils.Obj2Long('100'))
        assertEquals(res, NumericUtils.Obj2Long(100))
        assertEquals(res, NumericUtils.Obj2Long(100L))
        assertEquals(res, NumericUtils.Obj2Long(BigDecimal.valueOf(100L)))
        assertEquals(res, NumericUtils.Obj2Long(BigInteger.valueOf(100L)))
    }

    @Test
    void testObj2BigInteger() {
        def res = BigInteger.valueOf(100L)
        assertNull(NumericUtils.Obj2BigInteger(null))
        assertNull(NumericUtils.Obj2BigInteger(''))
        assertEquals(res, NumericUtils.Obj2BigInteger('100'))
        assertEquals(res, NumericUtils.Obj2BigInteger(100))
        assertEquals(res, NumericUtils.Obj2BigInteger(100L))
        assertEquals(res, NumericUtils.Obj2BigInteger(BigDecimal.valueOf(100L)))
        assertEquals(res, NumericUtils.Obj2BigInteger(BigInteger.valueOf(100L)))
    }

    @Test
    void testObj2BigDecimal() {
        def res = BigDecimal.valueOf(100L)
        assertNull(NumericUtils.Obj2BigDecimal(null))
        assertNull(NumericUtils.Obj2BigDecimal(''))
        assertEquals(res, NumericUtils.Obj2BigDecimal('100'))
        assertEquals(res, NumericUtils.Obj2BigDecimal(100))
        assertEquals(res, NumericUtils.Obj2BigDecimal(100L))
        assertEquals(res, NumericUtils.Obj2BigDecimal(BigDecimal.valueOf(100L)))
        assertEquals(res, NumericUtils.Obj2BigDecimal(BigInteger.valueOf(100L)))
    }
}
