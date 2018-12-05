package getl.utils

class MapUtilsTest extends getl.test.GetlTest {
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

    void testFindSection() {
        def m = [a: 1, b: [d: [f: 5], e: 4, g:[h: [i: [j: 6]]]], c: 3]

        assertNull(MapUtils.FindSection(m, 'a'))
        assertEquals(4, MapUtils.FindSection(m, 'b').e)
        assertEquals(5, MapUtils.FindSection(m, 'b.d').f)
        assertEquals(5, MapUtils.FindSection(m, 'b.*').f)
        assertEquals(6, MapUtils.FindSection(m, 'b.g.*.*').j)
    }

    void testXsdApi() {
        if (!FileUtils.ExistsFile('tests/xero/xero-accounting-api-schema-0.1.2.jar')) return

        def classLoader = FileUtils.ClassLoaderFromPath('tests/xero/xero-accounting-api-schema-0.1.2.jar')
        Class.forName('com.xero.model.Item', true, classLoader)

        def m = MapUtils.XsdFromResource( classLoader,'XeroSchemas/v2.00', 'Items.xsd')
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
}
