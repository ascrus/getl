package getl.utils

import getl.test.GetlTest
import org.junit.Test

class MapUtilsTest extends GetlTest {
    @Test
    void testMergeMap() {
        def map = [a: 1, b: 0, c: [ca: 1, cb: 2], d: [1, 2, 3], e: [[ea: 1, eb: 2], [ec: 3, ed: 4]]]
        def added = [b: 2, f: 3, g: 4, c: [cc: 3], d: [4, 5], e: [[ee: 5]]]
        MapUtils.MergeMap(map, added, true,true)
        def req = '''{
    "a": 1,
    "b": 2,
    "c": {
        "ca": 1,
        "cb": 2,
        "cc": 3
    },
    "d": [
        1,
        2,
        3,
        4,
        5
    ],
    "e": [
        {
            "ea": 1,
            "eb": 2
        },
        {
            "ec": 3,
            "ed": 4
        },
        {
            "ee": 5
        }
    ],
    "f": 3,
    "g": 4
}'''
        assertEquals(req, MapUtils.ToJson(map))
    }

    @Test
    void testFindSection() {
        def m = [a: 1, b: [d: [f: 5], e: 4, g:[h: [i: [j: 6]]]], c: 3]

        assertNull(MapUtils.FindSection(m, 'a'))
        assertEquals(4, MapUtils.FindSection(m, 'b').e)
        assertEquals(5, MapUtils.FindSection(m, 'b.d').f)
        assertEquals(5, MapUtils.FindSection(m, 'b.*').f)
        assertEquals(6, MapUtils.FindSection(m, 'b.g.*.*').j)
    }

    @Test
    void testXsdApi() {
        if (!FileUtils.ExistsFile('tests/xero/xero-accounting-api-schema-0.1.2.jar')) return

        def m = MapUtils.XsdFromResource( /*classLoader,*/'XeroSchemas/v2.00', 'Items.xsd')
        def f = MapUtils.XsdMap2Fields(m, 'Item')
        assertEquals(26, f.size())
        def unitPrice = f.find {it.name == 'PurchaseDetails.UnitPrice'}
        assertNotNull(unitPrice)
        assertEquals(20, unitPrice.length)
        assertEquals(2, unitPrice.precision)

        /*
        def m = MapUtils.XsdFromResource('/XeroSchemas/v2.00', 'Contact.xsd')
        def f = MapUtils.XsdMap2Fields(m, 'Contact')
        f.each { println it.name + '(' + it.type + '): ' + it.extended }
        */
    }

    @Test
    void testProcessArguments() {
        String[] a1 = ['a=1', '', 'b=2', ' ', 'c=3']
        assertEquals([a:1, b:2, c:3].toString(), MapUtils.ProcessArguments(a1).toString())

        String[] a2 = ['-a', '1', '-b', '2', ' ', '-c', '3']
        assertEquals([a:1, b:2, c:3].toString(), MapUtils.ProcessArguments(a2).toString())

        List<String> l1 = ['a=1', '', 'b=2', ' ', 'c=3']
        assertEquals([a:1, b:2, c:3].toString(), MapUtils.ProcessArguments(l1).toString())

        List<String> l2 = ['-a', '1', '-b', '2', ' ', '-c', '3']
        assertEquals([a:1, b:2, c:3].toString(), MapUtils.ProcessArguments(l2).toString())

        String s1 = ' a=1 2  3 '
        assertEquals([a:'1 2  3'].toString(), MapUtils.ProcessArguments(s1).toString())

        String s2 = ' -a 1 2  3 '
        assertEquals(['a 1 2  3':null].toString(), MapUtils.ProcessArguments(s2).toString())
    }

    @Test
    void testFindKeys() {
        def m = [a: 1, b: [c: 1, d: 2, e: [f: [1, 2], g: 3], h: [f: [1, 2], g: 3], j: 3], k: 2,
                 l: [c: 1, d: 2, e: [f: [1, 2], g: 3]]]
        def count = 0
        MapUtils.FindKeys(m, '*.*.f') { Map map, String key, item ->
            count++
            assertEquals('f', key)
            assertEquals([1, 2], item)
            map.put('_' + key, map.remove(key))
        }
        assertEquals(3, count)

        def res = '''{
    "a": 1,
    "b": {
        "c": 1,
        "d": 2,
        "e": {
            "g": 3,
            "_f": [
                1,
                2
            ]
        },
        "h": {
            "g": 3,
            "_f": [
                1,
                2
            ]
        },
        "j": 3
    },
    "k": 2,
    "l": {
        "c": 1,
        "d": 2,
        "e": {
            "g": 3,
            "_f": [
                1,
                2
            ]
        }
    }
}'''

        assertEquals(res, MapUtils.ToJson(m))
    }

    @Test
    void testConvertString2Object() {
        def m = [s: 'abc', numbers: [i: '100', n: '123.45'], dates:[d: '2019-12-31', dt: '2019-12-31 23:59:59'], other: 999]
        def r = MapUtils.ConvertString2Object(m)
        def tz = Calendar.instance.zoneOffset.hours
        def tzs = ((tz >= 0)?'+':'-') + StringUtils.AddLedZeroStr(tz, 2)
        def s = """{
    "s": "abc",
    "numbers": {
        "i": 100,
        "n": 123.45
    },
    "dates": {
        "d": "2019-12-31T00:00:00${tzs}00",
        "dt": "2019-12-31T23:59:59${tzs}00"
    },
    "other": 999
}""".toString()
        assertEquals(s, MapUtils.ToJson(r))
    }

    @Test
    void testClosure2Map() {
        def map = [a: 1, b: 'a', c: [1, 2, 3],
                   d: [i1: 1, i2: DateUtils.ParseDate('2019-12-31'), i3: [1, 2, 3], i4: [aa: 111]]]

        def cl = {
            a = 1
            b = 'a'
            c = [1,2, 3]
            d {
                i1 = 1
                i2 = DateUtils.ParseDate('2019-12-31')
                i3 = [1, 2, 3]
                i4 {
                    aa = 111
                }
            }
        }
        def res1 = MapUtils.Closure2Map {
            a = 1
            b = 'a'
            c = [1,2, 3]
            d {
                i1 = 1
                i2 = DateUtils.ParseDate('2019-12-31')
                i3 = [1, 2, 3]
                i4 {
                    aa = 111
                }
            }
        }

        assertEquals(map, res1)
        assertNull(res1.d.i4.bbb)

        def res2 = MapUtils.Closure2Map('env2') {
            environments {
                env1 {
                    map = 'incorrect!'
                }
                env2 {
                    a = 1; b = 'a'; c = [1,2,3]; d = [i1: 1, i2: DateUtils.ParseDate('2019-12-31'), i3: [1, 2, 3], i4: [aa: 111]]
                }
            }
        }
        assertEquals(map, res2)
    }

    @Test
    void testUnknown() {
        def a = ['one', 'two', 'three']
        assertEquals(0, MapUtils.Unknown([one: 1, three: 3, two: 2], a).size())
        assertEquals(['four'], MapUtils.Unknown([one: 1, three: 3, four: 4, two: 2], a))
        assertEquals(0, MapUtils.Unknown([one: 1, three: 3, _four: 4, two: 2], a, true).size())
        shouldFail {
            MapUtils.CheckKeys([one: 1, three: 3, four: 4, two: 2], a)
        }
    }

    @Test
    void testCompareMap() {
        def a = [a:1, b: [1,2,3], c: [d: 1, e: [1,2,3], f: [g: 1]], h: null]
        def b = [a:null, b: [1,2,3], c: [d: 1, e: [1,2,3], j: [g: 1]], h: 1]
        assertTrue(MapUtils.CompareMap(a, a).isEmpty())
        assertTrue(MapUtils.CompareMap(b, b).isEmpty())

        def res = '''{
    "a [unequal]": "1 <==> null",
    "c [unequal]": {
        "f [missing]": {
            "g [missing]": 1
        },
        "j [excess]": {
            "g [excess]": 1
        }
    },
    "h [excess]": 1
}'''
        assertEquals(res, MapUtils.ToJson(MapUtils.CompareMap(a, b)))
    }

    @Test
    void testClone() {
        def mo = [a: 1, b: 2, c: [d:3, e: 4], f: [1,2,3]]
        def mc = MapUtils.Clone(mo)
        mc.b = 20
        mc.c.e = 40
        mc.f << 4
        assertEquals([a: 1, b: 2, c: [d:3, e: 4], f: [1,2,3]], mo)
        assertEquals([a: 1, b: 20, c: [d:3, e: 40], f: [1,2,3,4]], mc)
    }

    @Test
    void testFindElement() {
        def m = [a: 1, b: 2, 'sub.c': 3, 'sub.d': 4]
        assertEquals(m, MapUtils.FindNodes(m))
        assertEquals([ 'sub.c': 3, 'sub.d': 4], MapUtils.FindNodes(m, 'sub.*'))
        shouldFail { MapUtils.FindSubNodes(m) }
        assertEquals([c: 3, d: 4], MapUtils.FindSubNodes(m, 'sub'))
    }
}