package getl.json

import getl.lang.Getl
import getl.test.GetlTest
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class JsonTest extends GetlTest {
    @Test
    void testRead() {
        Getl.Dsl(this) {
            json {
                fileName = 'resource:/json/customers.json'

                rootNode = 'customers'

                field('id') { type = integerFieldType }
                field('name')
                field('customer_type')
                field('phones') { type = objectFieldType } // Phones are stored as array list values and will be manual parsing

                def i = 0
                eachRow { row ->
                    i++
                    assertEquals(i, row.id)
                    assertEquals("Customer $i", row.name)
                    assertTrue(!(row.phones as List).isEmpty())
                }
                assertEquals(3, readRows)

                assertEquals(1, rows(limit: 1).size())
            }
        }
    }
}