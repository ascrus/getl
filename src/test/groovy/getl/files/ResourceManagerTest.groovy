package getl.files

import getl.files.sub.ResourceCatalogElem
import getl.lang.Getl
import getl.lang.sub.RepositoryConnections
import getl.test.GetlDslTest
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class ResourceManagerTest extends GetlDslTest {
    private void testListDir(ResourceCatalogElem rootNode) {
        def catalog = rootNode.files
        assertEquals(6, catalog.size())
        def repCon = catalog.find { it.filename == 'getl.lang.sub.RepositoryConnections' && it.type == Manager.directoryType }
        assertNotNull(repCon)
        assertEquals(2, repCon.files.size())
        assertEquals(rootNode, repCon.parent)
        assertEquals('/getl.lang.sub.RepositoryConnections', repCon.filepath)
        def h2Group = repCon.files.find { it.filename == 'h2' && it.type == Manager.directoryType }
        assertNotNull(h2Group)
        assertEquals(2, h2Group.files.size())
        assertEquals(repCon, h2Group.parent)
        assertEquals('/getl.lang.sub.RepositoryConnections/h2', h2Group.filepath)
        def h2con = h2Group.files.find { it.filename == 'getl_con.dev.conf' }
        assertNotNull(h2con)
        assertEquals(h2Group, h2con.parent)
        assertEquals('/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf', h2con.filepath)
    }

    @Test
    void testReadFromFiles() {
        testListDir(ResourceManager.ListDirFiles('src/test/resources/repository'))
    }

    @Test
    void testReadFromJar() {
        def file = FileUtils.FileFromResources('/reference/zip/repository.jar')
        testListDir(ResourceManager.ListDirJar("file:$file!/repository"))
    }

    @Test
    void testDownload() {
        Getl.Dsl {
            resourceFiles {
                useResourcePath '/repository'
                connect()

                def resFile = new File(localDirectory + '/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf')

                def conFile = '/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()

                changeDirectory '/getl.lang.sub.RepositoryConnections'

                conFile = '/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()

                conFile = 'h2/getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()

                changeDirectory 'h2'

                conFile = '/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()

                conFile = '../h2/getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()

                conFile = 'getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()
            }
        }
    }

    @Test
    void testBuildList() {
        Getl.Dsl {
            resourceFiles {
                useResourcePath '/repository'
                connect()

                def list = buildListFiles('*.conf') { recursive = true }
                assertEquals(14, list.countRow())
                assertTrue(existsDirectory('/' + RepositoryConnections.name))
                assertTrue(existsDirectory(RepositoryConnections.name))

                rootPath = '/' + RepositoryConnections.name
                list = buildListFiles('*.conf') { recursive = true }
                assertEquals(4, list.countRow())
                assertTrue(existsDirectory('/' + RepositoryConnections.name))
                assertTrue(existsDirectory('../' + RepositoryConnections.name))
            }
        }
    }
}