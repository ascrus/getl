package getl.models

import getl.jdbc.TableDataset
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

            embeddedTable('story', true) {
                type = localTemporaryTableType
            }

            embeddedTable('points', true) {
                type = localTemporaryTableType
            }

            embeddedTable('master', true) {
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                create()
            }

            embeddedTable('detail', true) {
                type = localTemporaryTableType
                field('master_id') { type = integerFieldType; isNull = false }
                field('value') { type = integerFieldType }
                create()
            }

            arrayDataset('#values', true) {
                field('value') { type = integerFieldType }
            }

            def model = models.mapTables('map1', true) {
                useSourceConnection 'csv:con'
                useDestinationConnection 'h2:con'
                useStoryDataset embeddedTable('story')
                useIncrementDataset embeddedTable('points')

                mapTable('file') {
                    linkTo 'master'
                    incrementFieldName = 'ID'
                    scripts.before = 'DELETE FROM {dest_table}'
                }

                mapTable('#values') {
                    linkTo 'detail'
                    attachToParentDataset 'file', 'values'
                    map.master_id = 'id'
                    scripts.before = 'DELETE FROM {dest_table}'
                }
            }

            model.tap {
                clearIncrementDataset()
                etl.copyRows(mapTable('file').source, mapTable('file').destination) { fc ->
                    fc.beforeWrite {
                        def sql = mapTable('file').scripts.before
                        (fc.destination as TableDataset).currentJDBCConnection.executeCommand(sql, [queryParams: [dest_table: fc.destination.objectFullName]])
                    }

                    childs(mapTable('#values').destination) { cf ->
                        linkSource = arrayDataset('#values')
                        linkField = mapTable('#values').parentLinkFieldName
                        map = mapTable('#values').map

                        cf.prepareChild {
                            def sql = mapTable('#values').scripts.before
                            (cf.dataset as TableDataset).currentJDBCConnection.executeCommand(sql, [queryParams: [dest_table: cf.dataset.objectFullName]])
                        }
                    }

                    requiredStatistics = ['ID']

                    fc.afterWrite {
                        mapTable('file').historyPointObject.saveValue(statistics.id.maximumValue)
                    }
                }
                assertEquals(1, incrementDataset.countRow())
                assertEquals(3, mapTable('file').historyPointObject.lastValue())
                clearIncrementDataset()
                assertEquals(0, incrementDataset.countRow())
            }

            def csvRows = csvTemp('file').rows()
            assertEquals([id: 1, name: 'test1', values: [1,2,3]], csvRows[0])
            assertEquals([id: 2, name: 'test2', values: [4,5,6]], csvRows[1])
            assertEquals([id: 3, name: 'test3', values: [7,8,9]], csvRows[2])

            csvTemp('file').eachRow { r ->
                assertEquals(1, embeddedTable('master').countRow('id = {id} AND name = \'{name}\'', [id: r.id, name: r.name]))
                (r.values as List).each { num ->
                    assertEquals(1, embeddedTable('detail').countRow('master_id = {id} AND value = {value}', [id: r.id, value: num]))
                }
            }
        }
    }
}