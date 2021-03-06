package getl.yaml

import getl.lang.Getl
import getl.test.TestDsl
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class YamlTest extends TestDsl {
    @Test
    void testData() {
        Getl.Dsl {
            yaml('yaml:file1', true) {
                fileName = 'resource:/yaml/test.yaml'
                codePage = 'utf-8'
                rootNode = 'PriceCalculationRule'

                field('code')
                field('name')
                field('specCode')
                field('charValueUse') { type = objectFieldType }
                field('dependency')

                def rows = rows()
                assertEquals(158, rows.size())
                rows.each {r ->
                    assertNotNull(r.code)
                    assertNotNull(r.name)
                }
            }
        }
    }
}