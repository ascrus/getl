package getl.files

import getl.files.sub.ResourceCatalogElem
import getl.lang.Getl
import getl.lang.sub.RepositoryConnections
import getl.test.TestDsl
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class ResourceManagerTest extends TestDsl {
    static private void testListDir(ResourceCatalogElem rootNode) {
        def catalog = rootNode.files
        assertEquals(7, catalog.size())
        def repCon = catalog.find { it.filename == 'getl.lang.sub.RepositoryConnections' && it.type == Manager.directoryType }
        assertNotNull(repCon)
        assertEquals(4, repCon.files.size())
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

                def resFile = new File(localDirectory + '/getl_con.dev.conf')

                def conFile = '/getl.lang.sub.RepositoryConnections/h2/getl_con.dev.conf'
                download(conFile)
                assertTrue(resFile.exists())
                assertTrue(getLastModified(conFile) > 0)
                resFile.delete()

                changeDirectory ('/getl.lang.sub.RepositoryConnections')

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

                changeDirectory ('h2')

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
                assertEquals(24, list.countRow())
                assertTrue(existsDirectory('/' + RepositoryConnections.name))
                assertTrue(existsDirectory(RepositoryConnections.name))

                rootPath = '/' + RepositoryConnections.name
                list = buildListFiles('*.conf') { recursive = true }
                assertEquals(8, list.countRow())
                assertTrue(existsDirectory('/' + RepositoryConnections.name))
                assertTrue(existsDirectory('../' + RepositoryConnections.name))
            }
        }
    }

    @Test
    void testListJarFile() {
        Getl.Dsl {
            def jarFileName = FileUtils.ResourceFileName('resource:/jars/test.jar')
            def jarFile = new File(jarFileName)
            def jarLoader = FileUtils.ClassLoaderFromPath(jarFileName)
            resourceFiles {
                useResourcePath jarFileName
                useClassLoader jarLoader
                connect()
                def list = buildListFiles('*.*') { recursive = true }
                assertEquals(5, list.countRow())
                assertEquals(5, list.select('SELECT DISTINCT filepath FROM {table}').size())
                assertTrue(existsFile('file.txt'))
                download('file.txt')
                removeLocalFile('file.txt')
                assertTrue(existsFile('test1/file.txt'))
                assertTrue(existsFile('test1/test2/file.txt'))
                assertTrue(existsFile('test1/test2/test3/file.txt'))
                download('test1/test2/test3/file.txt', localDirectory + '/test1/test2/test3/file.txt')
                removeLocalFile('test1/test2/test3/file.txt')
            }
            jarLoader.close()
            jarLoader = null
            System.gc()
            sleep 1000
            println jarFile.delete()
        }
    }

    @Test
    void testBadNames() {
        Getl.Dsl {
            def configName = 'easyportal.conf'
            def jarFile = new File('tests/jar files/easyloader test.jar')
            def classLoader = new URLClassLoader(new URL[] { jarFile.toURI().toURL() }, null as ClassLoader)
            resourceFiles {
                useClassLoader classLoader
                useResourcePath FileUtils.ConvertToUnixPath(jarFile.path)

                assertTrue(existsFile(configName))
                download(configName)
                def f = new File("${localDirectory}/$configName")
                def opts = getl.config.ConfigSlurper.LoadConfigFile(f)
                assertEquals('easyloader', opts.name)

                def files = buildListFiles('*.class') { recursive = true }
                assertEquals(1, files.countRow('filepath = \'ru/easydata/easyloader/launcher\' AND filename = \'Job.class\''))
            }
        }
    }
}