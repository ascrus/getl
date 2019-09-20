package getl.postgresql

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import org.junit.Test

class PostgreSQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/postgresql/postgresql.conf'

	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		def con = new PostgreSQLConnection(config: 'postgresql')
		defaultSchema = 'public'
		return con
	}

	@Test
	void testLimit() {
		def count = 1000
		Getl.Dsl(this) {
			usePostgresqlConnection registerConnection(this.con, 'getl.test', true)

			postgresqlTable('test_limit', true) {
				tableName = 'getl_test_limit'
				field('id') { type = integerFieldType; isKey = true }
				field('name') { length = 50; isNull = false }
				field('dt') { type = datetimeFieldType; isNull = false }
				createOpts { ifNotExists = true }
				writeOpts { batchSize = 500 }
				create()
				truncate()
			}

			rowsTo(postgresqlTable('test_limit')) {
				writeRow { add ->
					(1..count).each {
						add id: it, name: "test $it", dt: DateUtils.now
					}
				}
			}

			assertEquals(count, postgresqlTable('test_limit').countRow())

			rowProcess(postgresqlTable('test_limit')) {
				int c = 0
				readRow {
					c++
				}
				assertEquals(count, c)
			}

			rowProcess(postgresqlTable('test_limit')) {
				sourceParams.limit = 1
				int c = 0
				readRow { c++ }
				assertEquals(1, c)
			}
		}
		Config.ReInit()
	}
}