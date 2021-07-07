package getl.utils

import getl.test.GetlTest
import org.junit.Test

class ConvertUtilsTest extends GetlTest {
    @Test
    void testString2Structure() {
        def m = [a: 1, b:'a', c:[1, 2, 'a']]
        def t = "a:1, b:'a', c:[1,2,'a']"
        assertEquals(m, ConvertUtils.String2Structure(t))
        assertEquals(m, ConvertUtils.String2Structure('[' + t + ']'))
        assertEquals(m, ConvertUtils.String2Structure('{' + t + '}'))
    }
}
