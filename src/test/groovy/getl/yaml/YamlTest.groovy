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

                def r = rows()
                assertEquals(158, r.size())
                r.each {
                    assertNotNull(it.code)
                    assertNotNull(it.name)
                }

                def fr = rows(filter: { it.code == 'PCR-ANY-COMP-NEWBASEMON3-VOICE-MA46' })
                assertEquals(1, fr.size())
                assertEquals('PCR-ANY-COMP-NEWBASEMON3-VOICE-MA46', fr[0].code)

                readOpts.filter { it.code == 'PCR-ANY-COMP-NEWBASEMON3-VOICE-MA46' }
                fr = rows()
                assertEquals(1, fr.size())
                assertEquals('PCR-ANY-COMP-NEWBASEMON3-VOICE-MA46', fr[0].code)
            }
        }
    }
}