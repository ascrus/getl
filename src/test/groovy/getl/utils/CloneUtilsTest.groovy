package getl.utils

import groovy.json.*
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class CloneUtilsTest extends getl.test.GetlTest {
    private Map exampleObjectByJson() {
        def parser = new JsonSlurper()
        def s = '''
{
    "map": {
        "test": "test",
        "list": [1,2,3,4,5, { "a": 11, "b": 12, "c": 13 }],
        "submap": {
            "a": 1,
            "b": 2,
            "c": 3,
            "sublist": [21,22,23]
        }
    },
    "list": ["a", "b", "c"],
    "prop": "value"
}
'''

        return parser.parseText(s) as Map
    }

    private Map exampleObjectByMap() {
        return [
                map: [
                        test: 'test',
                        list: [1,2,3,4,5, [a:11, b:12, c:13]],
                        submap: [a:1, b:2, c:3, sublist:[21,22,23]]
                ],
                list: ['a', 'b', 'c'],
                prop: 'value'
        ]
    }

    private void validClone(Map c, Map n) {
        n.map.test = "complete"
        n.map.list[0] = 11
        n.map.list[5].a = 111
        n.map.submap.a = 11
        n.map.submap.sublist[0] = 211
        n.list[0] = "aa"
        n.prop = "ok"

        assertFalse(c.map == n.map)
        assertFalse(c.map.list == n.map.list)
        assertFalse(c.map.submap == n.map.submap)
        assertFalse(c.map.submap.sublist == n.map.submap.sublist)
        assertFalse(c.list == n.list)
        assertFalse(c.map.list[0] == n.map.list[0])
        assertFalse(c.map.list[5].a == n.map.list[5].a)
        assertFalse(c.map.submap.a == n.map.submap.a)
        assertFalse(c.map.submap.sublist[0] == n.map.submap.sublist[0])
        assertFalse(c.list[0] == n.list[0])
        assertFalse(c.prop == n.prop)
    }

    @Test
    void testClone() {
        def c = exampleObjectByJson()
        def n = CloneUtils.CloneObject(c)

        validClone(c, n)
    }

    @Test
    void testStreamClone() {
        def c = exampleObjectByMap()
        def n = CloneUtils.StreamClone(c)

        validClone(c, n)
    }
}
