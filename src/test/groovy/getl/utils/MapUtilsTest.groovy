package getl.utils

class MapUtilsTest extends GroovyTestCase {
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
}
