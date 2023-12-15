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
				tableName = 'Test_Unlogged'
				field('ID') { type = integerFieldType; isKey = true }
				field('Name') { length = 50; isNull = false }
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

	@Test
	void testEnumType() {
		Getl.Dsl {
			def tab = postgresqlTable {
				setConnection(con)
				schemaName = 'public'
				tableName = 'test_enum'
			}

			if (!tab.exists) {
				sql(con) {
					exec '''
CREATE TYPE public.e_enum_type AS ENUM (
    'NONE',
    'SINGLE',
    'ALL'
);

CREATE TABLE public.test_enum (
    id int NOT NULL PRIMARY KEY,
    name varchar(50) NOT NULL,
    type e_enum_type NOT NULL
);
'''
				}
			}

			tab.tap {
				retrieveFields()
				field.each { println it }

				truncate()
			}

			def r = [
					[id: 1, name: 'Type NONE', type: 'NONE'],
					[id: 2, name: 'Type SINGLE', type: 'SINGLE'],
					[id: 3, name: 'Type SINGLE', type: 'ALL']
			]

			etl.rowsTo(tab) {
				writeRow { add ->
					r.each { add.call(it) }
				}
			}
			assertEquals(r, tab.rows(order: ['id']))

			def tabCopy = postgresqlTable {
				setConnection(con)
				schemaName = tab.schemaName()
				tableName = tab.tableName + '_copy'
				field = tab.field
				createOpts { ifNotExists = true }
				create()
				truncate()
			}
			etl.copyRows(tab, tabCopy)
			assertEquals(r, tabCopy.rows(order: ['id']))

			def tabExport = postgresqlTable {
				setConnection(con)
				schemaName = tab.schemaName()
				tableName = tab.tableName + '_export'
				importFields(tab)
				createOpts { ifNotExists = true }
				create()
				truncate()
			}
			etl.copyRows(tab, tabExport)
			assertEquals(r, tabExport.rows(order: ['id']))
		}
	}

	@Test
	void testCaseFields() {
		Getl.Dsl {
			def c = con.cloneConnection() as PostgreSQLConnection
			c.caseSensitiveFields = true
			def tab1 = postgresqlTable {
				setConnection c
				tableName = 'Test_Case_Fields'
				field('id') { type = integerFieldType; isKey = true }
				field('Name') { length = 50; isNull = false }
				field('DT') { type = datetimeFieldType }

				drop(ifExists: true)
				create()
			}

			PostgreSQLTable tab2

			try {
				tab2 = postgresqlTable {
					setConnection c
					tableName = tab1.tableName
					retrieveFields()
				}

				tab1.field.each { f1 ->
					def f2 = tab2.fieldByName(f1.name)
					assertNotNull(f2)
					assertEquals(f1.name, f2.name)
					assertEquals(f1.type, f2.type)
					assertEquals(f1.length, f2.length)
					assertEquals(f1.isKey, f2.isKey)
					assertEquals(f1.isNull, f2.isNull)
				}
			}
			finally {
				tab1.drop()
			}

			c.caseSensitiveFields = null
			tab1.create()
			try {
				tab2.retrieveFields()
				tab1.field.each { f1 ->
					def f2 = tab2.fieldByName(f1.name)
					assertNotNull(f2)
					assertEquals(f1.name.toLowerCase(), f2.name)
					assertEquals(f1.type, f2.type)
					assertEquals(f1.length, f2.length)
					assertEquals(f1.isKey, f2.isKey)
					assertEquals(f1.isNull, f2.isNull)
				}
			}
			finally {
				tab1.drop()
			}
		}
	}

	@Test
	void testCaseWork() {
		Getl.Dsl {
			def c = con.cloneConnection() as PostgreSQLConnection
			def proc = { String ddlCreateTable ->
				def t = postgresqlTable {
					setConnection c
					tableName = 'Test_Case_Work'
					field('ID') { type = integerFieldType; isKey = true }
					field('Name') { length = 50; isNull = false }
					field('value') { type = numericFieldType; length = 5; precision = 2 }
					drop(ifExists: true)
					if (ddlCreateTable == null)
						create()
					else
						c.executeCommand(ddlCreateTable)
				}

				try {
					def data = [] as List<Map>
					(1..3).each { data << [id: it, name: "Test $it", value: it + 0.12] }

					etl.rowsTo(t) {
						writeRow { add ->
							data.each { add(it) }
						}
					}
					assertEquals(3, t.countRow())

					def i = 0
					t.eachRow {
						assertEquals(data[i].id, it.id)
						assertEquals(data[i].name, it.name)
						assertEquals(data[i].value, it.value)
						i++
					}

					def f = csvTempWithDataset(t)
					etl.copyRows(t, f)
					assertEquals(3, t.readRows)
					assertEquals(3, f.writeRows)

					t.truncate()
					assertEquals(0, t.countRow())

					t.bulkLoadCsv(f)
					assertEquals(3, t.countRow())

					i = 0
					t.eachRow {
						assertEquals(data[i].id, it.id)
						assertEquals(data[i].name, it.name)
						assertEquals(data[i].value, it.value)
						i++
					}
				}
				finally {
					t.drop()
				}
			}

			c.saveToHistory('-- *** Lower case name ')
			proc()

			c.saveToHistory('-- *** Sensitive name with sensitive name in table')
			c.caseSensitiveFields = true
			proc('CREATE TABLE "Test_Case_Work" ("ID" int PRIMARY KEY, "Name" varchar(50) NOT NULL, value numeric(5, 2))')

			c.saveToHistory('-- *** Sensitive name ')
			c.caseSensitiveFields = true
			proc()
		}
	}

	@Override
	protected Boolean synchronizeStructureTable()  { true }

	@Override
	protected String schemaForSynchronizeStructureTable() { 'public' }
}