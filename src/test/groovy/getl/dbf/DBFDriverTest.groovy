package getl.dbf

import getl.lang.Getl
import getl.test.GetlDslTest
import org.junit.Before
import org.junit.Test

class DBFDriverTest extends GetlDslTest {
    @Before
    void initDbf() {
        Getl.Dsl {
            dbfConnection('dbf:con', true) {
                path = 'tests/dbf'
                extension = 'dbf'
            }
            dbf('dbf:cp1251', true) {
                useConnection dbfConnection('dbf:con')
                it.fileName = 'cp1251'
                it.codePage = 'cp1251'
            }

            dbf('dbf:bdays', true) {
                useConnection dbfConnection('dbf:con')
                it.fileName = 'bdays'
            }

            ['8c', '8b', 'f5', '30', '31'].each { n ->
                dbf("dbf:dbase_$n", true) {
                    useConnection dbfConnection('dbf:con')
                    it.fileName = "dbase_$n"
                }
            }

            dbf('dbf:dbase_8b').fileMemoExtension = 'dbt'
            dbf('dbf:dbase_30').fileMemoExtension = 'fpt'
            dbf('dbf:dbase_f5').fileMemoExtension = 'fpt'
        }
    }

    @Test
    void testReadCp1251() {
        Getl.Dsl {
            dbf('dbf:cp1251') {
                def rows = rows()
                assertEquals(4, rows.size())
                assertEquals('[name:амбулаторно-поликлиническое, rn:1]', rows[0].toString())
                assertEquals('[name:больничное, rn:2]', rows[1].toString())
                assertEquals('[name:НИИ, rn:3]', rows[2].toString())
                assertEquals('[name:образовательное медицинское учреждение, rn:4]', rows[3].toString())
            }
        }
    }

    @Test
    void testReadRows() {
        Getl.Dsl {
            processDatasets('dbf:*') {  name ->
                dbf(name) {
                    retrieveFields()
                    rows(limit: 10)
                }
            }
        }
    }
}