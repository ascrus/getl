package getl.utils

import org.junit.Test

class ListUtilsTest extends getl.test.GetlTest {

    private void list2str(List list) {
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
}
