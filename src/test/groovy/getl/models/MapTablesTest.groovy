package getl.models

import getl.lang.Getl
import getl.test.TestDsl
import org.junit.Test

class MapTablesTest extends TestDsl {
    @Test
    void testMapModel() {
        Getl.Dsl {
            useCsvTempConnection csvTempConnection('csv:con', true)
            csvTemp('file', true) {
                fileName = 'file1'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('values') { type = arrayFieldType; arrayType = 'INT' }

                etl.rowsTo {
                    writeRow { add ->
                        add id: 1, name: 'test1', values: [1,2,3]
                        add id: 2, name: 'test2', values: [4,5,6]
                        add id: 3, name: 'test3', values: [7,8,9]
                    }
                }
            }

            embeddedConnection('h2:con', true)
            useEmbeddedConnection embeddedConnection('h2:con')
            embeddedTable('master', true) {
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                create()
            }

            embeddedTable('detail', true) {
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isNull = false }
                field('value') { type = integerFieldType }
                create()
            }

            arrayDataset('#values', true) {
                field('value') { type = integerFieldType }
            }

            def model = models.mapTables('map1', true) {
                useSourceConnection 'csv:con'
                useDestinationConnection 'h2:con'

                mapTable('file') {
                    linkTo 'master'
                }

                mapTable('#values') {
                    linkTo 'detail'
                    attachToParentDataset 'file', 'values'
                }
            }

            model.tap {
                etl.copyRows(mapTable('file').source, mapTable('file').destination) {
                    childs(mapTable('#values').destination) {
                        linkSource = arrayDataset('#values')
                        linkField = mapTable('#values').parentLinkFieldName
                    }
                }
            }

            def csvRows = csvTemp('file').rows()
            assertEquals([id: 1, name: 'test1', values: [1,2,3]], csvRows[0])
            assertEquals([id: 2, name: 'test2', values: [4,5,6]], csvRows[1])
            assertEquals([id: 3, name: 'test3', values: [7,8,9]], csvRows[2])

            csvTemp('file').eachRow { r ->
                assertEquals(1, embeddedTable('master').countRow('id = {id} AND name = \'{name}\'', [id: r.id, name: r.name]))
                (r.values as List).each { num ->
                    assertEquals(1, embeddedTable('detail').countRow('id = {id} AND value = {value}', [id: r.id, value: num]))
                }
            }
        }
    }
}