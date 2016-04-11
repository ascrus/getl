package getl.excel

import getl.data.Field

class ExcelDriverTest extends GroovyTestCase {
    private static final String path = 'tests/excel'
    private static final String fileName = 'test.xlsx'
    private static ExcelConnection connection = new ExcelConnection(path: path, fileName: fileName)
    private static ExcelDataset excelDataset
    private static final String listName = 'test'

    void setUp() {
        excelDataset = new ExcelDataset(connection: connection, header: true)
        excelDataset.field << new Field(name: 'a', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'b', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'c', type: Field.Type.STRING)

        excelDataset.listName = listName
    }

    void testEachRow() {
		println "testEachRow ..."
		
        def counter = 0
        excelDataset.eachRow {
            counter++
			println it
        }

        assertEquals(2, counter)
    }

    void testLimit() {
		println "testLimit ..."
		
        excelDataset.limit = 1
        assertEquals(1, excelDataset.rows().size())
    }

    void testHeader() {
		println "testHeader ..."
		
		try {
	        excelDataset.header = true
	        assertEquals(true, excelDataset.header)
	
	        excelDataset.header = false
	        assertEquals(false, excelDataset.header)
		}
		finally {
			excelDataset.header = true
		}
    }

    void testNullHeader() {
		println "testNullHeader ..."
        assertEquals(true, excelDataset.header)
    }

    void testHeaderResults() {
		println "testHeaderResults ..."
		
        assertEquals(2, excelDataset.rows().size())
    }

    void testListName() {
		println "testListName ..."
		
        assertEquals(listName, excelDataset.listName)

        excelDataset.listName = null
        excelDataset.rows()
    }

    void testListNameAsZero() {
		println "testListNameAsZero ..."
		
        excelDataset.listName = 0
    }
}
