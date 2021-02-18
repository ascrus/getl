package getl.postgresql

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.BeforeClass
import org.junit.Test

@InheritConstructors
class PostgreSQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/postgresql/postgresql.conf'

	@BeforeClass
	static void CleanGetl() {
		Getl.CleanGetl()
	}

	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		def con = new PostgreSQLConnection(config: 'postgresql')
		defaultSchema = 'public'
		needCatalog = con.connectDatabase
		return con
	}

	@Test
	void testLimit() {
		def count = 1000
		Getl.Dsl(this) {
			usePostgresqlConnection registerConnectionObject(this.con, 'getl.test.postgresql', true) as PostgreSQLConnection

			def table = postgresqlTable('getl.test.postgresql.test_limit', true) {
				tableName = 'getl_test_limit'
				field('id') { type = integerFieldType; isKey = true }
				field('name') { length = 50; isNull = false }
				field('dt') { type = datetimeFieldType; isNull = false }
				createOpts { ifNotExists = true }
//				writeOpts { batchSize = 500 }
				create()
				truncate()
			}

			etl.rowsTo(table) {
				writeRow { add ->
					(1..count).each {
						add id: it, name: "test $it", dt: DateUtils.now
					}
				}
			}

			assertEquals(count, table.countRow())

			etl.rowsProcess(table) {
				int c = 0
				readRow {
					c++
				}
				assertEquals(count, c)
			}

			etl.rowsProcess(table) {
				sourceParams.limit = 1
				int c = 0
				readRow { c++ }
				assertEquals(1, c)
			}
		}
		Config.ReInit()
	}

	@Override
	protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }
}