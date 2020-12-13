package getl.utils

import getl.data.Field
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class PathTest extends getl.test.GetlTest {
    static final def maskStr = '/root/{group}/{subgroup}/test_{date}_{num}.txt'
    static final Path path = new Path().with {
        mask = this.maskStr
        variable('date') { type = dateFieldType; format = 'yyyy-MM-dd' }
        variable('num') { type = integerFieldType; length = 2 }
        variable('year') {
            calc { (it.date != null)?DateUtils.PartOfDate('year', it.date as Date):(null as Integer) }
        }
        variable('field1') { type = stringFieldType; length = 50 }
        compile()

        return it
    }

    @Test
    void testCompile() {
        def p = path.clone() as Path
        assertEquals('/root/{group}/{subgroup}/test_{date}_{num}.txt', p.maskStr)
        assertEquals('/root', p.rootPath)
        assertEquals(5, p.countLevel)
        assertEquals(2, p.numLocalPath)
        assertEquals('(?i)/root/(.+)/(.+)/test_(\\d\\d\\d\\d-\\d\\d-\\d\\d)_(\\d{2})[.]txt', p.maskPath)
        assertEquals('(?i)/root/(.+)/(.+)/(.+)', p.maskFolder)
        assertEquals('/root/${group}/${subgroup}/test_${date}_${num}.txt', p.maskFile)
        assertEquals('/root/%/%', p.likeFolder)
        assertEquals('/root/%/%/test_%_%\\.txt', p.likeFile)
        assertEquals([vars:[], mask:'', like:''], p.elements[0])
        assertEquals([vars:[], mask:'root', like:'root'], p.elements[1])
        assertEquals([vars:['group'], mask:'(.+)', like:'%'], p.elements[2])
        assertEquals([vars:['subgroup'], mask:'(.+)', like:'%'], p.elements[3])
        assertEquals([vars:['date', 'num'], mask:'test_(\\d\\d\\d\\d-\\d\\d-\\d\\d)_(\\d{2})[.]txt', like:'%'], p.elements[4])
        assertNotNull(p.vars.group)
        assertNotNull(p.vars.subgroup)
        assertEquals('yyyy-MM-dd', p.vars.date?.format)
        assertEquals(2, p.vars.num.len)
        assertEquals(p.variable('year').calc, p.vars.year?.calc)
        assertNotNull(p.vars.field1)
        assertEquals(Field.stringFieldType, p.vars.field1.type)
        assertEquals(50, p.vars.field1.len)
    }

    @Test
    void testAnalyzeDir() {
        def p = path.clonePath()
        def m = p.analyzeDir('/root/group test/state ok')
        assertEquals('group test', m.group)
        assertEquals('state ok', m.subgroup)
        assertNull(m.year)

        def n = p.analyzeDir('/group test/state ok')
        assertNull(n)
    }

    @Test
    void testAnalyzeFile() {
        def p = path.clonePath()
        def m = p.analyzeFile('/root/group test/state ok/test_2016-10-15_41.txt')
        assertEquals('group test', m.group)
        assertEquals('state ok', m.subgroup)
        assertEquals(DateUtils.ParseDate('2016-10-15'), m.date)
        assertEquals(41, m.num)
        assertEquals(2016, m.year)

        shouldFail { p.analyzeFile('/root/group test/state ok/test_2016-13-15_41.txt') }
        assertNull(p.analyzeFile('/root/group test/state ok/test_2016-10-15_a41.txt'))

        p.ignoreConvertError = true
        assertNull(p.analyzeFile('/root/group test/state ok/test_2016-13-15_41.txt'))
        assertNull(p.analyzeFile('/root/group test/state ok/test_2016-10-15_041.txt'))

        p.ignoreConvertError = false
        p.variable('num') { length = null }
        p.compile(mask: maskStr, vars: [date: [type: 'DATE', format: 'yyyy-MM-dd'], num: [type: 'INTEGER']])
        shouldFail { p.analyzeFile('/root/group test/state ok/test_2016-10-15_11234567890.txt') }
    }

    @Test
    void testGenerateFileName() {
        def p = path.clonePath()
        def v = [group: 'group test', subgroup: 'state ok', date: DateUtils.ParseDate('2016-10-15'), num: 41]
        assertEquals('/root/group test/state ok/test_2016-10-15_41.txt', p.generateFileName(v, true))

        v = [group: 'group test', subgroup: 'state ok', date: '2016-10-15', num: '41']
        assertEquals('/root/group test/state ok/test_2016-10-15_41.txt', p.generateFileName(v, false))
    }

    @Test
    void testMasks2Paths() {
        def l = ['a*', 'b{var1}', 'c']
        def r = Path.Masks2Paths(l)
        for (int i = 0; i < l.size(); i++) {
            assertEquals(l[i], r[i].mask)
        }
        assertTrue(Path.MatchList('abc', r))
        assertTrue(Path.MatchList('b123', r))
        assertTrue(Path.MatchList('c', r))
        assertFalse(Path.MatchList('d', r))
    }
}