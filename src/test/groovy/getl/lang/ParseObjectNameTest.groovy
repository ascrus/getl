package getl.lang

import getl.lang.sub.ParseObjectName
import getl.test.GetlDslTest
import org.junit.Test

class ParseObjectNameTest extends GetlDslTest {
    @Test
    void testLastSubsgroupName() {
        Getl.Dsl {
            assertEquals('subgroup', parseName('root_group.group_1.subgroup:').lastSubgroup())
            assertEquals('group', parseName('group:object').lastSubgroup())
            assertNull(parseName('object').lastSubgroup())
        }
    }

    @Test
    void testSubgroup2Name() {
        Getl.Dsl {
            assertEquals('root_group.group_1:subgroup', ParseObjectName.Subgroup2Object('root_group.group_1.subgroup'))
            assertNull(parseName(null).lastSubgroup())
            shouldFail { ParseObjectName.Subgroup2Object('root_group.group_1.subgroup:object') }
        }
    }

    @Test
    void testSearchMask() {
        Getl.Dsl {
            assertEquals('group:*', parseName('group:object').searchMask())
            assertEquals('group:obj*', parseName('group:object').searchMask('obj*'))
            assertEquals('*', parseName('object').searchMask())
            assertEquals('group:*', parseName('group:').searchMask())
        }
    }
}