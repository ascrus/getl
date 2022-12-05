package getl.oracle

import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.jdbc.TableDataset
import getl.lang.Getl
import getl.proc.Flow
import getl.tfs.TDS
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import oracle.sql.TIMESTAMPTZ
import org.h2.value.ValueTimestampTimeZone
import org.junit.Test

import java.sql.Timestamp

@InheritConstructors
class OracleDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/oracle/oracle.conf'
	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		Locale.setDefault(new Locale('en','EN'))
		return new OracleConnection(config: 'oracle')
	}

	@Test
	void testTZ() {
		def ds = new OracleTable(connection: con)
		ds.tableName = 'test_tz'
		ds.field << new Field(name: 'id', type: Field.integerFieldType, isKey: true)
		ds.field << new Field(name: 'dt', type: Field.datetimeFieldType)
		ds.field << new Field(name: 'dtz', type: Field.timestamp_with_timezoneFieldType)
		ds.drop(ifExists: true)
		ds.create()
		try {
			new Flow().writeTo(dest: ds) { updater ->
				Map r = [:]
				r.id = 1
				def d = new Date()
				r.dt = d
				r.dtz = d
				updater(r)
			}

			ds.field.clear()

			(ds.connection as JDBCConnection).sqlConnection.eachRow('select * from ' + ds.fullTableName) { r ->
				def t = r.dtz
				println t
				def x = new Timestamp((t as TIMESTAMPTZ).timestampValue(con.sqlConnection.connection).getTime())
				println x
			}

			ds.eachRow { r ->
				println r
			}
		}
		finally {
			ds.drop()
		}
	}

	@Override
	protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }

	@Test
	void testSelectFromFunc() {
		Getl.Dsl {
			def ds = oracleTable {
				useConnection(con as OracleConnection)
				schemaName = 'developer'
				tableName = 'test_func'
			}
			if (!ds.exists) {
				sql {
					useConnection con
					exec '''CREATE TABLE test_func (
    id int NOT NULL PRIMARY KEY,
    name varchar(50) NOT NULL,
    d1 date NOT NULL,
    d2 timestamp NOT NULL,
    d3 timestamp WITH TIME ZONE NOT NULL,
    d4 timestamp WITH LOCAL TIME ZONE NOT NULL
);'''
				}
			}
			ds.retrieveFields()
			//ds.field.each { println it }
			assertEquals([name: 'ID', type: 'NUMERIC', typeName: 'NUMBER', length: 38, precision: 0, isNull: false, isKey: true, ordKey: 1], ds.fieldByName('id').toMap())
			assertEquals(6, ds.field.size())
			assertEquals([name: 'NAME', type: 'STRING', typeName: 'VARCHAR2', length: 50, isNull: false], ds.fieldByName('name').toMap())
			assertEquals([name: 'D1', type: 'DATETIME', typeName: 'DATE', isNull: false], ds.fieldByName('d1').toMap())
			assertEquals([name: 'D2', type: 'DATETIME', typeName: 'TIMESTAMP(6)', isNull: false], ds.fieldByName('d2').toMap())
			assertEquals([name: 'D3', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP(6) WITH TIME ZONE', isNull: false], ds.fieldByName('d3').toMap())
			assertEquals([name: 'D4', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP(6) WITH LOCAL TIME ZONE', isNull: false], ds.fieldByName('d4').toMap())

			ds.truncate()

			def d = DateUtils.CurrentDate()
			def dt = DateUtils.Now()
			etl.rowsTo(ds) {
				writeRow { add ->
					(1..9).each { num ->
						add id: num, name: "row $num", d1: d, d2: dt, d3: dt, d4: dt
					}
				}
			}

			ds.field.clear()
			def rows = ds.rows()
			//ds.field.each { println it }
			assertEquals(6, ds.field.size())
			assertEquals([name: 'ID', type: 'NUMERIC', typeName: 'NUMBER', length: 38, precision: 0, isNull: false, columnClassName: 'java.math.BigDecimal'], ds.fieldByName('id').toMap())
			assertEquals([name: 'NAME', type: 'STRING', typeName: 'VARCHAR2', length: 50, isNull: false, columnClassName: 'java.lang.String'], ds.fieldByName('name').toMap())
			assertEquals([name: 'D1', type: 'DATETIME', typeName: 'DATE', isNull: false, columnClassName: 'java.sql.Timestamp'], ds.fieldByName('d1').toMap())
			assertEquals([name: 'D2', type: 'DATETIME', typeName: 'TIMESTAMP', isNull: false, columnClassName: 'oracle.sql.TIMESTAMP'], ds.fieldByName('d2').toMap())
			assertEquals([name: 'D3', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP WITH TIME ZONE', isNull: false, columnClassName: 'oracle.sql.TIMESTAMPTZ'], ds.fieldByName('d3').toMap())
			assertEquals([name: 'D4', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP WITH LOCAL TIME ZONE', isNull: false, columnClassName: 'oracle.sql.TIMESTAMPLTZ'], ds.fieldByName('d4').toMap())

			(1..9).each { num ->
				def r = rows[num - 1]
				assertEquals(num, (r.id as Number).toInteger())
				assertEquals("row $num".toString(), r.name)
				assertEquals(d, r.d1)
				assertEquals(dt, r.d2)
				assertEquals(dt, r.d3)
				assertEquals(dt, r.d4)
			}

			query { qry ->
				useConnection con
				query = 'SELECT * FROM test_func ORDER BY 1'
				rows = qry.rows()

				//ds.field.each { println it }
				assertEquals(6, field.size())
				assertEquals([name: 'ID', type: 'NUMERIC', typeName: 'NUMBER', length: 38, precision: 0, isNull: false, columnClassName: 'java.math.BigDecimal'], fieldByName('id').toMap())
				assertEquals([name: 'NAME', type: 'STRING', typeName: 'VARCHAR2', length: 50, isNull: false, columnClassName: 'java.lang.String'], fieldByName('name').toMap())
				assertEquals([name: 'D1', type: 'DATETIME', typeName: 'DATE', isNull: false, columnClassName: 'java.sql.Timestamp'], fieldByName('d1').toMap())
				assertEquals([name: 'D2', type: 'DATETIME', typeName: 'TIMESTAMP', isNull: false, columnClassName: 'oracle.sql.TIMESTAMP'], fieldByName('d2').toMap())
				assertEquals([name: 'D3', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP WITH TIME ZONE', isNull: false, columnClassName: 'oracle.sql.TIMESTAMPTZ'], fieldByName('d3').toMap())
				assertEquals([name: 'D4', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP WITH LOCAL TIME ZONE', isNull: false, columnClassName: 'oracle.sql.TIMESTAMPLTZ'], fieldByName('d4').toMap())

				(1..9).each { num ->
					def r = rows[num - 1]
					assertEquals(num, (r.id as Number).toInteger())
					assertEquals("row $num".toString(), r.name)
					assertEquals(d, r.d1)
					assertEquals(dt, r.d2)
					assertEquals(dt, r.d3)
					assertEquals(dt, r.d4)
				}
			}

			query { qry ->
				useConnection con
				query = 'SELECT * FROM TABLE(DEVELOPER.P_TEST_FUNC.testFunc(1, 9)) ORDER BY 1'
				rows = qry.rows()

				//ds.field.each { println it }
				assertEquals(6, field.size())
				assertEquals([name: 'ID', type: 'NUMERIC', typeName: 'NUMBER', length: 0, precision: 0, columnClassName: 'java.math.BigDecimal'], fieldByName('id').toMap())
				assertEquals([name: 'NAME', type: 'STRING', typeName: 'VARCHAR2', length: 50, columnClassName: 'java.lang.String'], fieldByName('name').toMap())
				assertEquals([name: 'D1', type: 'DATETIME', typeName: 'DATE', columnClassName: 'java.sql.Timestamp'], fieldByName('d1').toMap())
				assertEquals([name: 'D2', type: 'DATETIME', typeName: 'TIMESTAMP', columnClassName: 'oracle.sql.TIMESTAMP'], fieldByName('d2').toMap())
				assertEquals([name: 'D3', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP WITH TIME ZONE', columnClassName: 'oracle.sql.TIMESTAMPTZ'], fieldByName('d3').toMap())
				assertEquals([name: 'D4', type: 'TIMESTAMP_WITH_TIMEZONE', typeName: 'TIMESTAMP WITH LOCAL TIME ZONE', columnClassName: 'oracle.sql.TIMESTAMPLTZ'], fieldByName('d4').toMap())

				(1..9).each { num ->
					def r = rows[num - 1]
					assertEquals(num, (r.id as Number).toInteger())
					assertEquals("row $num".toString(), r.name)
					assertEquals(d, r.d1)
					assertEquals(dt, r.d2)
					assertEquals(dt, r.d3)
					assertEquals(dt, r.d4)
				}
			}
		}
	}
}