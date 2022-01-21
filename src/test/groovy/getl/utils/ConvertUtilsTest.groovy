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
        assertEquals(m, ConvertUtils.String2Structure('(' + t + ')'))

        def l = [1,2,3]
        def s = '1,2,3'
        assertEquals(l, (ConvertUtils.String2Structure(s) as List<String>).collect { Integer.valueOf(it) })
        assertEquals(l, ConvertUtils.String2Structure('[' + s + ']'))
        assertEquals(l, ConvertUtils.String2Structure('{' + s + '}'))
        assertEquals(l, ConvertUtils.String2Structure('(' + s + ')'))

        l = ['a', 'b', 'c']
        s = 'a, b, c'
        assertEquals(l, ConvertUtils.String2Structure(s))

        m = [a: 'aaa', b: 'bbb', c: 'ccc']
        t = 'a:aaa, b: bbb, c: ccc'
        assertEquals(m, ConvertUtils.String2Structure(t))
    }

    @Test
    void testString2List() {
        def l = ConvertUtils.String2List('1:1, 2, 3, 4')
        assert l[0] instanceof String
        assertEquals('1:1', l[0])
        assert l[1] instanceof Integer
        assertEquals(2, l[1])

        l =  ConvertUtils.String2List('\'1\', 2, 3, \'4\'')
        assert l[0] instanceof String
        assertEquals('1', l[0])
        assert l[1] instanceof Integer
        assertEquals(2, l[1])

        l = ConvertUtils.String2List('a:1,b:2,c:3,d:4')
        assertEquals(['a:1', 'b:2', 'c:3', 'd:4'], l)

        l =  ConvertUtils.String2List('\'1, 2\', 3000000000,\'4\'')
        assert l.size() == 3
        assert l[0] instanceof String
        assertEquals('1, 2', l[0])
        assert l[1] instanceof Long
        assertEquals(3000000000, l[1])
        assert l[2] instanceof String
        assertEquals('4', l[2])

        l =  ConvertUtils.String2List('[\'1, 2\', 3.12,\'4\']')
        assert l.size() == 3
        assert l[0] instanceof String
        assertEquals('1, 2', l[0])
        assert l[1] instanceof BigDecimal
        assertEquals(3.12, l[1])
        assert l[2] instanceof String
        assertEquals('4', l[2])

        l = ConvertUtils.String2List("['ONE', 'TWO', 'THREE']")
        assertEquals(3, l.size())
        assertEquals(['ONE', 'TWO', 'THREE'], l)
    }

    @Test
    void testString2Map() {
        def l = ConvertUtils.String2Map('a: 1, b:2, c : 3, d:')
        assertEquals([a:1, b:2, c:3, d:null], l)

        l = ConvertUtils.String2Map('\'a a\': \'aa\', "b":" b b ", \' c \' : " c c ", "d:d, e:e": " d d : e , e "')
        assertEquals(['a a': 'aa', b: ' b b ', ' c ': ' c c ', 'd:d, e:e': ' d d : e , e '], l)

        l = ConvertUtils.String2Map("list: [ONE, TWO, THREE]")
        assertEquals([list: '[ONE, TWO, THREE]'], l)

        l = ConvertUtils.String2Map("'list' :[ONE, TWO, THREE] , a : 1")
        assertEquals([list: '[ONE, TWO, THREE]', a: 1], l)

        l = ConvertUtils.String2Map('a : 1, "list" : [ONE, TWO, THREE]')
        assertEquals([a: 1, list: '[ONE, TWO, THREE]'], l)

        l = ConvertUtils.String2Map("a: 'a', list: ['ONE', 'TWO', 'THREE'], b: 'b'")
        assertEquals([a: 'a', list: "['ONE', 'TWO', 'THREE']", b: 'b'], l)
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
