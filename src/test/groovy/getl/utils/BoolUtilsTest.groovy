package getl.utils

/**
 * @author Alexsey Konstantinov
 */
class BoolUtilsTest extends GroovyTestCase {
    void testIsValue() {
        assertTrue(BoolUtils.IsValue(true, false))
        assertTrue(BoolUtils.IsValue(null, true))
        assertTrue(BoolUtils.IsValue('True', false))
        assertFalse(BoolUtils.IsValue('False', true))
        assertTrue(BoolUtils.IsValue('ON', false))
        assertFalse(BoolUtils.IsValue('off', true))
        assertTrue(BoolUtils.IsValue(1, false))
        assertFalse(BoolUtils.IsValue(0, true))
        assertTrue(BoolUtils.IsValue([null, null, true], false))
        assertTrue(BoolUtils.IsValue([null, null, 'true'], false))
        assertTrue(BoolUtils.IsValue([null, null, 1], false))
        shouldFail { BoolUtils.IsValue('a', true)}
        shouldFail { BoolUtils.IsValue(['a'], true)}
    }

    void testClassInstanceOf() {
        assertTrue(BoolUtils.ClassInstanceOf(java.sql.Timestamp, Date))
    }
}
