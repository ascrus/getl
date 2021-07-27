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

    @Test
    void testVar2Object() {
        assertNull(ConvertUtils.Var2Object(null, [a:1, b: 'test']))
        assertEquals(1, ConvertUtils.Var2Object('{a}', [a:1, b: 'test']))
        assertEquals('test', ConvertUtils.Var2Object('{b}', [a:1, b: 'test']))
        assertEquals('none', ConvertUtils.Var2Object('none', [a:1, b: 'test']))
        shouldFail { ConvertUtils.Var2Object('{unknown}', [a:1, b: 'test']) }
        assertEquals('{unknown}', ConvertUtils.Var2Object('{unknown}', [a:1, b: 'test'], false))
    }
}
