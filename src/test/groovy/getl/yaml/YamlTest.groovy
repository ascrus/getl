package getl.yaml

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.test.GetlTest
import getl.utils.FileUtils
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
import groovy.yaml.YamlSlurper
import org.junit.Test

@InheritConstructors
class YamlTest extends GetlDslTest {
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

                eachRow { row ->
                    println row
                }
            }
        }
    }
}
