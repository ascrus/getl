package getl.xml

import getl.lang.Getl
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class XmlTest extends getl.test.GetlTest {
    @Test
    void testRead() {
        Getl.Dsl(this) {
            xml {
                fileName = 'resource:/xml/customers.xml'

                rootNode = 'list:customers.list:customer'
                defaultAccessMethod = DEFAULT_NODE_ACCESS // Fields values are stored as node value

                attributeField('version') { alias = 'header.@version[0]' }
                attributeField('objecttype') { alias = 'header.@object[0]' }
                attributeField('time') { type = timestamp_with_timezoneFieldType; alias = 'header.sender.@time[0]' }

                field('id') { type = integerFieldType }
                field('name')
                field('customer_type') { alias = '@'} // Customer value are stored as attribute value
                field('phones') { type = objectFieldType } // Phones are stored as array list values and will be manual parsing

                uniFormatDateTime = 'yyyy-MM-dd\'T\'HH:mm:ssXXX'

                def i = 0
                eachRow { row ->
                    i++
                    assertEquals(i, row.id)
                    assertEquals("Customer $i".toString(), row.name)
                    assertNotNull(row.customer_type)
                    assertTrue(!(row.phones as List).isEmpty())
                }
                assertEquals(3, readRows)
                assertEquals('1.00', attributeValue.version)
                assertEquals('Customers', attributeValue.objecttype)
                assertEquals(getl.utils.DateUtils.ParseSQLTimestamp("yyyy-MM-dd'T'HH:mm:ssXXX", '2019-01-02T01:02:03+03:00'), attributeValue.time)

                assertEquals(1, rows(limit: 1).size())
            }
        }
    }
}