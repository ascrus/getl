package getl.excel

import getl.data.Field

class ExcelDriverTest extends GroovyTestCase {
    public static final String path = '/Users/DShaldin/Documents'
    public static final String fileName = 'test.xlsx'

    void testEachRow() {
        println "Path: $path"
        println "File Name: $fileName"

        ExcelConnection connection = new ExcelConnection(path: path, fileName: fileName)
        ExcelDataset excelDataset = new ExcelDataset(connection: connection, listName: 'test')

        excelDataset.field << new Field(name: 'a', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'b', type: Field.Type.INTEGER)
        excelDataset.field << new Field(name: 'c', type: Field.Type.STRING)

        println excelDataset.rows()
    }
}
