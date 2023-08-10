package getl.utils

import getl.data.Field
import getl.test.GetlTest
import org.junit.Test

import java.sql.Date

/**
 * @author Alexsey Konstantinov
 */
class PathTest extends GetlTest {
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
        assertEquals('(?iu)/root/(.+)/(.+)/test_(\\d\\d\\d\\d-\\d\\d-\\d\\d)_(\\d{2})[.]txt', p.maskPath)
        assertEquals('(?iu)/root/(.+)/(.+)/(.+)', p.maskFolder)
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
        assertEquals(p.variable('year').onCalc, p.vars.year?.calc)
        assertNull(p.vars.field1)
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

    @Test
    void testCreateFromDescription() {
        def desc = '/{dir}/{date}/{name}.{num}.{part}.csv?dir||/\\\\d{2}.+/;date|date|yyyyMMdd;name||\\|\\;|50;num|integer|||1|3;part|datetime|yyyy-MM-dd HH:mm:ss'
        def m = new Path(desc)

        def valid = {
            assertEquals('/{dir}/{date}/{name}.{num}.{part}.csv', m.mask)
            assertEquals(['date', 'dir', 'name', 'num', 'part'], m.vars.keySet().toList().sort())

            assertEquals('\\d{2}.+', m.vars.dir.regular)

            assertEquals('|;', m.vars.name.format)
            assertEquals(50, m.vars.name.len)

            assertEquals(Field.datetimeFieldType, m.vars.part.type)
            assertEquals('yyyy-MM-dd HH:mm:ss', m.vars.part.format)

            assertEquals(Field.integerFieldType, m.vars.num.type)
            assertEquals(1, m.vars.num.lenMin)
            assertEquals(3, m.vars.num.lenMax)

            assertEquals(Field.dateFieldType, m.vars.date.type)
            assertEquals('yyyyMMdd', m.vars.date.format)

            assertEquals(desc, m.toString())
        }
        valid()

        m = m.clonePath()
        valid()
    }

    @Test
    void testToString() {
        assertEquals('{a}.{b}.*', new Path('{a}.{b}.*').toString())
        assertEquals('{a}.{b}.*', new Path('{a}.{b}.*?a;b').toString())
        assertEquals('{a}.{b}.*?a|date|yyyy-MM-dd', new Path('{a}.{b}.*?a|date').toString())
        assertEquals('{a}.{b}.*?a|date|yyyyMMdd', new Path('{a}.{b}.*?a|date|yyyyMMdd').toString())
        assertEquals('{a}.{b}.*?b|integer||10', new Path('{a}.{b}.*?b|integer||10;').toString())
        assertEquals('{a}.{b}.*?a||/.+/;b||||1|2', new Path('{a}.{b}.*?a||/.+/;b||||1|2').toString())
    }

    @Test
    void testClone() {
        def o = new Path('{region}/{date}/*.{num}.csv?region||MSK\\|SPB\\|KRD;date|date|yyyy-MM-dd;num|integer')
        def vo = o.analyzeFile('KRD/2023-07-15/file1.001.csv')
        assertEquals('KRD', vo.region)
        assertEquals(DateUtils.ParseSQLDate('2023-07-15'), vo.date as Date)
        assertEquals(1, vo.num)

        def n = o.clone() as Path
        assertEquals(o.toString(), n.toString())
        def vn = n.analyzeFile('KRD/2023-07-15/file1.001.csv')
        assertEquals(vo, vn)
        assertEquals('KRD', vn.region)
        assertEquals(DateUtils.ParseSQLDate('2023-07-15'), vn.date as Date)
        assertEquals(1, vn.num)
    }

    @Test
    void testFormat2Regexp() {
        assertEquals('MSK|SPB|KRD', Path.Format2Regexp('MSK|SPB|KRD'))
        assertEquals('MSK|SPB|KRD', Path.Format2Regexp('(MSK|SPB|KRD)'))
        assertEquals('(MSK|SPB)|(KRD)', Path.Format2Regexp('(MSK|SPB)|(KRD)'))
        assertEquals('MSK|[(]SPB[)]|KRD', Path.Format2Regexp('MSK|(SPB)|KRD'))
    }

    @Test
    void testUnicodeMatch() {
        def p = new Path('*Тест*')
        assertTrue(p.match('первое тестирование юникод'))
        assertTrue(p.match('Второе Тестирование юникод'))
        assertTrue(p.match('ТРЕТЬЕ ТЕСТИРОВАНИЕ ЮНИКОД'))
    }
}