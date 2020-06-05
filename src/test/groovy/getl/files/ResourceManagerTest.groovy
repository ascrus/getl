package getl.files

import getl.lang.Getl
import getl.test.GetlDslTest
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class ResourceManagerTest extends GetlDslTest {
    @Test
    void testReadFiles() {
        def catalog = ResourceManager.ListDirFiles('src/test/resources/repository')
        assertEquals(6, catalog.size())
        def repCon = catalog.find { it.filename == 'getl.lang.sub.RepositoryConnections' && it.type == Manager.directoryType }
        assertNotNull(repCon)
        assertEquals(2, repCon.files.size())
        assertNull(repCon.parent)
        assertEquals('/getl.lang.sub.RepositoryConnections', repCon.filepath)
        def h2Group = repCon.files.find { it.filename == 'h2' && it.type == Manager.directoryType }
        assertNotNull(h2Group)
        assertEquals(1, h2Group.files.size())
        assertEquals(repCon, h2Group.parent)
        assertEquals('/getl.lang.sub.RepositoryConnections/h2', h2Group.filepath)
        def h2con = h2Group.files[0]
        assertEquals('getl_con.dev.conf', h2con.filename)
        assertEquals(h2Group, h2con.parent)
        assertEquals('/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf', h2con.filepath)
    }

    @Test
    void testOperations() {
        Getl.Dsl {
            resourceFiles {
                try {
                    def conFile = '/repository/getl.lang.sub.RepositoryConnections/getl_con.dev.conf'
                    download(conFile)
                    assertTrue(new File(localDirectory + conFile).exists())

                    rootPath = '/repository'
                    def dsFile = 'getl.lang.sub.RepositoryDatasets/getl_table.conf'
                    download(dsFile)
                    assertTrue(new File(localDirectory + '/' + dsFile).exists())
                }
                finally {
                    FileUtils.DeleteFolder(localDirectoryFile.canonicalPath, true)
                }
            }
        }
    }
}
