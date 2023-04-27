package getl.postgresql

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.StringUtils
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

	@Override
	protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }

	@Override
	protected String getUseArrayType() { 'int4' }

	@Override
	void prepareTable() {
		table.fieldByName('id2').length = 6
		table.fieldByName('dtwithtz').length = 6
	}

	@Test
	void testUnloggedTable() {
		Getl.Dsl {
			postgresqlTable {
				setConnection(con)
				tableName = 'test_unlogged'
				field('id') { type = integerFieldType; isKey = true }
				field('name') { length = 50; isNull = false }
				dropOpts {
					ifExists = true
				}
				createOpts {
					ifNotExists = true
					unlogged = true
				}
				writeOpts {
					batchSize = 1
				}
				drop()
				create()
				assertTrue(exists)
				assertEquals(0, countRow())
				etl.rowsTo {
					writeRow { add ->
						add id: 1, name: 'test 1'
					}
				}
				assertEquals(1, countRow())
				assertEquals([id: 1, name: 'test 1'], rows()[0])

				try {
					etl.rowsTo {
						writeRow { add ->
							add id: 2, name: 'test 2'
							abortWithError('Rollback insert')
						}
					}
				} catch (Exception ignored) { }
				assertEquals(1, countRow())
			}
		}
	}
}