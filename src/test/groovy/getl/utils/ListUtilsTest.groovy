package getl.utils

import getl.test.GetlTest
import org.junit.Test

class ListUtilsTest extends GetlTest {

    private static void list2str(List list) {
        def str = ListUtils.List2StrArray(list)
        def parseList = ListUtils.StrArray2List(str, Integer)
        assertEquals(list, parseList)
//        println "$str: $parseList"
    }

    @Test
    void testList2StrArray() {
        list2str([])
        list2str([1])
        list2str([1,2])
        list2str([1,2,4])
        list2str([1,3,4])
        list2str([1,3,4,6])
        list2str([1,2,4,5,7,8])
        list2str([1,3,4,6,8,9,11])
        list2str([1,2,3,4,5,6,7,8,9,10])
    }

    @Test
    void testSplitList() {
        def l = [1,2,3,4,5,6,7,8,9]
        def r = ListUtils.SplitList(l, 4)
        assertEquals([[1,5,9], [2,6], [3,7], [4,8]], r)

        assertEquals([[1]], ListUtils.SplitList([1], 3))

        assertEquals([], ListUtils.SplitList([], 2))

        assertEquals([[1], [2]], ListUtils.SplitList([1,2,3], 3, 2))

        shouldFail { ListUtils.SplitList([1, 2, 3], 0) }
    }

    @Test
    void testToList() {
        assertNull(ListUtils.ToList(null))
        assertEquals([1,2,3], ListUtils.ToList([1,2,3]))
        assertEquals(['1','2','3'], ListUtils.ToList('1, 2, 3'))
    }
}