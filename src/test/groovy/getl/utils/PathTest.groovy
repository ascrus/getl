package getl.utils

/**
 * @author Alexsey Konstantinov
 */
class PathTest extends getl.test.GetlTest {
    static final def maskPath = '/root/{group}/{subgroup}/test_{date}_{num}.txt'

    void testCompile() {
        def p = new Path()
        p.compile(mask: maskPath)
        assertEquals('/root/{group}/{subgroup}/test_{date}_{num}.txt', p.maskStr)
        assertEquals('/root', p.rootPath)
        assertEquals(5, p.countLevel)
        assertEquals(2, p.numLocalPath)
        assertEquals('(?i)/root/(.+)/(.+)/test_(.+)_(.+)[.]txt', p.maskPath)
        assertEquals('(?i)/root/(.+)/(.+)/(.+)', p.maskFolder)
        assertEquals('/root/${group}/${subgroup}/test_${date}_${num}.txt', p.maskFile)
        assertEquals('/root/%/%', p.likeFolder)
        assertEquals('/root/%/%/test_%_%\\.txt', p.likeFile)
        assertEquals([group:[:], subgroup:[:], date:[:], num:[:]], p.vars)
        assertEquals([vars:[], mask:'', like:''], p.elements[0])
        assertEquals([vars:[], mask:'root', like:'root'], p.elements[1])
        assertEquals([vars:['group'], mask:'(.+)', like:'%'], p.elements[2])
        assertEquals([vars:['subgroup'], mask:'(.+)', like:'%'], p.elements[3])
        assertEquals([vars:['date', 'num'], mask:'test_(.+)_(.+)[.]txt', like:'%'], p.elements[4])
    }

    void testAnalizeDir() {
        def p = new Path()
        p.compile(mask: maskPath)
        def m = p.analizeDir('/root/group test/state ok')
        assertEquals('group test', m.group)
        assertEquals('state ok', m.subgroup)

        def n = p.analizeDir('/group test/state ok')
        assertNull(n)
    }

    void testAnalizeFile() {
        def p = new Path()
        p.compile(mask: maskPath, vars: [date: [type: 'DATE', format: 'yyyy-MM-dd'], num: [type: 'INTEGER', len: 2]])
        def m = p.analizeFile('/root/group test/state ok/test_2016-10-15_41.txt')
        assertEquals('group test', m.group)
        assertEquals('state ok', m.subgroup)
        assertEquals(DateUtils.ParseDate('2016-10-15'), m.date)
        assertEquals(41, m.num)

        shouldFail { p.analizeFile('/root/group test/state ok/test_2016-13-15_41.txt') }
        assertNull(p.analizeFile('/root/group test/state ok/test_2016-10-15_a41.txt'))

        p.ignoreConvertError = true
        assertNull(p.analizeFile('/root/group test/state ok/test_2016-13-15_41.txt'))
        assertNull(p.analizeFile('/root/group test/state ok/test_2016-10-15_041.txt'))

        p.ignoreConvertError = false
        p.vars.num.len = null
        p.compile(mask: maskPath, vars: [date: [type: 'DATE', format: 'yyyy-MM-dd'], num: [type: 'INTEGER']])
        shouldFail { p.analizeFile('/root/group test/state ok/test_2016-10-15_11234567890.txt') }
    }

    void testGenerateFileName() {
        def p = new Path()
        p.compile(mask: maskPath, vars: [date: [type: 'DATE', format: 'yyyy-MM-dd'], num: [type: 'INTEGER', len: 2]])
        def v = [group: 'group test', subgroup: 'state ok', date: DateUtils.ParseDate('2016-10-15'), num: 41]
        assertEquals('/root/group test/state ok/test_2016-10-15_41.txt', p.generateFileName(v, true))

        v = [group: 'group test', subgroup: 'state ok', date: '2016-10-15', num: '41']
        assertEquals('/root/group test/state ok/test_2016-10-15_41.txt', p.generateFileName(v, false))
    }
}
