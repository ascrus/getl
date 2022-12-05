package getl.exception

import getl.lang.Getl
import getl.test.TestDsl
import org.junit.Test

class ExceptionsTest extends TestDsl {
    private ExceptionGETL e = new NotSupportError()

    @Test
    void testExceptions() {
        assertEquals('Not support feature!', e.message)
        e = new NotSupportError('test')
        assertEquals('Not support "test"!', e.message)
        e = new NotSupportError('object', 'test', 'detail')
        assertEquals('Not support object "test" (detail)!', e.message)

        Getl.Dsl { getl ->
            embeddedTable('group:object', true) { tab ->
                schemaName = 'schema'
                tableName = 'table'
            }

            def check = {
                embeddedTable { tab ->
                    schemaName = 'schema'
                    tableName = 'table'
                    this.e = new NotSupportError(tab, 'object', 'test', 'detail')
                    assertEquals('TDSTable {"SCHEMA"."TABLE"}: Not support object "test" (detail)!', e.message)
                }

                embeddedTable('group:object') { tab ->
                    this.e = new NotSupportError(tab, 'object', 'test', 'detail')
                    assertEquals('TDSTable {group:object}: Not support object "test" (detail)!', e.message)

                    this.e = new DslError(getl, '#object.not_support', [type: 'object', feature: 'test', detail: 'detail'])
                    assertEquals('Script {DSL}: Not support object "test" (detail)!', e.message)
                }
            }
            check.call()
            options.language = 'en'
            check.call()
        }
    }
}