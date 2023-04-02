package getl.lang

import getl.lang.sub.ParseObjectName
import getl.test.GetlDslTest
import org.junit.Test

class ParseObjectNameTest extends GetlDslTest {
    @Test
    void testObjectName() {
        Getl.Dsl {
            assertEquals('object_1', parseName('Object_1').name)
            assertEquals('#object 1.2', parseName('#Object 1.2').name)
            shouldFail { parseName('Object^1').name }

            assertEquals('group1:object1', parseName('Group1:Object1').name)
            assertEquals('group1.group2:object1.object2 test', parseName('Group1.Group2:Object1.Object2 test').name)
            shouldFail { parseName('group1:object:1').name }
            shouldFail { parseName('group1..group2:object:1').name }
            shouldFail { parseName('group1/group2:object 1').name }
            shouldFail { parseName('group1.group2:object1\\object2').name }

            def p = parseName('group1:object1')
            p.groupName = 'Group1.Group2'
            p.objectName = 'Object1_Object2'
            assertEquals('group1.group2', p.groupName)
            assertEquals('object1_object2', p.objectName)
            assertEquals('group1.group2:object1_object2', p.name)

            assertTrue(ParseObjectName.Parse('object1.object2', false, false).validName())
            assertTrue(ParseObjectName.Parse('#object1.object2', false, false).validName())
            assertFalse(ParseObjectName.Parse('^object1.object2', false, false).validName())
            assertTrue(ParseObjectName.Parse('group1.group2:object1.object2', false, false).validName())
            assertTrue(ParseObjectName.Parse('group1.group2:object1.object2', false, false).validName())
            assertFalse(ParseObjectName.Parse('group1.%group2%:object1.object2', false, false).validName())
            assertFalse(ParseObjectName.Parse('group1.group2:object1.$object2$', false, false).validName())
            assertFalse(ParseObjectName.Parse('group1.:group2:object1.object2', false, false).validName())
            assertFalse(ParseObjectName.Parse('group1.group2:object1.:object2', false, false).validName())
        }
    }

    @Test
    void testLastSubsgroupName() {
        Getl.Dsl {
            assertEquals('subgroup', parseName('root_group.group_1.subgroup:').lastSubgroup)
            assertEquals('group', parseName('group:object').lastSubgroup)
            assertNull(parseName('object').lastSubgroup)
        }
    }

    @Test
    void testSubgroup2Name() {
        Getl.Dsl {
            assertEquals('root_group.group_1:subgroup', ParseObjectName.Subgroup2Object('root_group.group_1.subgroup'))
            assertNull(parseName(null).lastSubgroup)
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

    @Test
    void testConvertObjectName() {
        assertEquals('test_hex2F_123', ParseObjectName.toObjectName('test/123'))
        assertEquals('test__newline_123__hex27_a_hex27_', ParseObjectName.toObjectName('test_\n123_\'a\''))
    }
}