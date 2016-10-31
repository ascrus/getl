package getl.tfs

import getl.csv.CSVConnection
import getl.data.Field

/**
 * Created by ascru on 10.10.2016.
 */
class TFSTest extends GroovyTestCase {
    void testAnonymusFile () {
        print 'testAnonymusFile ['
        TFSDataset f = TFS.dataset()
        f.autoSchema = true
        assertEquals(f.connection.deleteOnExit, true)
        println f.fullFileName() + ']'
        f.field << new Field(name: 'name')

        f.openWrite()
        f.write([name: 'testAnonymusFile complete'])
        f.doneWrite()
        f.closeWrite()

        f.eachRow { println it }
    }

    void testNameFile () {
        println 'testNameFile'
        TFSDataset f = new TFS(path: TFS.systemPath).dataset('test_tfs.txt')
        f.autoSchema = true
        f.field << new Field(name: 'name')
        f.openWrite()
        f.write([name: 'testNameFile complete'])
        f.doneWrite()
        f.closeWrite()

        f.eachRow { println it }
    }
}
