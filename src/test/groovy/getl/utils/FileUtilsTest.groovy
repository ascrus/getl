package getl.utils

import getl.lang.Getl
import getl.test.GetlTest
import getl.tfs.TFS
import org.junit.Test

import java.sql.Timestamp

/**
 * @author Alexsey Konstantinov
 */
class FileUtilsTest extends GetlTest {
    @Test
    void testFileExtension() {
        assertEquals('txt', FileUtils.FileExtension('test.txt'))
        assertEquals('txt', FileUtils.FileExtension('/tmp/test.getl/test.txt'))
        assertEquals('file.txt',  FileUtils.AddExtension('file.txt', 'txt'))
        assertEquals('file.txt',  FileUtils.AddExtension('file', 'txt'))
        assertEquals('/tmp/file.txt',  FileUtils.AddExtension('/tmp/file.txt', 'txt'))
        assertEquals('/tmp/file.txt',  FileUtils.AddExtension('/tmp/file', 'txt'))
        assertEquals('/tmp.txt/file.txt',  FileUtils.AddExtension('/tmp.txt/file.txt', 'txt'))
        assertEquals('/tmp.txt/file.txt',  FileUtils.AddExtension('/tmp.txt/file', 'txt'))
    }

    @Test
    void testExcludeFileExtension() {
        assertEquals(FileUtils.ConvertToDefaultOSPath('test'), FileUtils.ExcludeFileExtension('test.txt'))
        assertEquals(FileUtils.ConvertToDefaultOSPath('/tmp/test.getl/test'), FileUtils.ExcludeFileExtension('/tmp/test.getl/test.txt'))
    }

    @Test
    void testRenameTo() {
        def source = "${TFS.systemPath}/${FileUtils.UniqueFileName()}"
        def destFileName = 'test_rename.txt'
        def dest = "${TFS.systemPath}/$destFileName"

        new File(source).text = 'test'
        FileUtils.RenameTo(source, destFileName)

        assertTrue(FileUtils.DeleteFile(dest))
    }

    @Test
    void testMoveTo() {
        def fileName = 'test_move.txt'
        def source = "${TFS.systemPath}/$fileName"
        def destPath = "${TFS.systemPath}/test_move"

        new File(source).text = 'test'
        shouldFail { FileUtils.MoveTo(source, destPath) }
        FileUtils.MoveTo(source, destPath, true)
        assertEquals('test', new File("$destPath/$fileName").text)

        new File(source).text = 'test 1'
        FileUtils.MoveTo(source, destPath)
        assertEquals('test 1', new File("$destPath/$fileName").text)

        assertTrue(FileUtils.DeleteFile("$destPath/$fileName"))
        assertTrue(FileUtils.DeleteDir(destPath))
    }

    @Test
    void testCopyToDir() {
        def fileName = 'test_copy.txt'
        def source = "${TFS.systemPath}/$fileName"
        def destPath = "${TFS.systemPath}/test_copy"

        def f = new File(source)
        f.deleteOnExit()
        f.text = 'test'

        shouldFail { FileUtils.CopyToDir(source, destPath) }
        FileUtils.CopyToDir(source, destPath, true)
        assertTrue(new File("$destPath/$fileName").exists())

        FileUtils.CopyToDir(f, destPath, "${fileName}.other")
        assertTrue(new File("$destPath/${fileName}.other").exists())

        assertTrue(FileUtils.DeleteFolder(destPath, true))
    }

    @Test
    void testCopyToFile() {
        def fileName = 'test_copy.txt'
        def source = "${TFS.systemPath}/$fileName"
        def destPath = "${TFS.systemPath}/test_copy"
        def destFileName = "$destPath/test_copy_new.txt"

        def f = new File(source)
        f.deleteOnExit()
        f.text = 'test'
        shouldFail { FileUtils.CopyToFile(source, destFileName) }
        FileUtils.CopyToFile(source, destFileName, true)

        assertTrue(FileUtils.DeleteFile(destFileName))
        assertTrue(FileUtils.DeleteDir(destPath))
    }

    @Test
    void testDeleteFolder() {
        def path = "${TFS.systemPath}/test_empty_folder"
        FileUtils.ValidPath(path)
        FileUtils.ValidPath("$path/1/a")
        FileUtils.ValidPath("$path/1/b")
        FileUtils.ValidPath("$path/1/c")
        FileUtils.ValidPath("$path/2")

        new File("$path/1/a/test_empty.txt").text = 'test'
        new File("$path/1/b/test_empty.txt").text = 'test'

        FileUtils.DeleteEmptyFolder(path, true)
        assertTrue(new File("$path/1").exists())
        assertFalse(new File("$path/2").exists())

        assertTrue(FileUtils.DeleteFolder("$path/1", true))
        assertFalse(new File("$path/1").exists())

        FileUtils.DeleteEmptyFolder(path, true)
        assertFalse(new File(path).exists())
    }

    @Test
    void testLastDirFromPath() {
        assertNull(FileUtils.lastDirFromPath(null as String))
        assertEquals('test.getl', FileUtils.lastDirFromPath("/tmp/test.getl"))
    }

    @Test
    void testLastDirFromFile() {
        assertNull(FileUtils.lastDirFromFile(null as String))

        def path = "${TFS.systemPath}/test.getl"
        def fileName = "$path/test.txt"

        FileUtils.ValidFilePath(fileName)
        new File(fileName).text = 'test'

        assertEquals('test.getl', FileUtils.lastDirFromFile(fileName))

        FileUtils.DeleteFolder(path, true)
    }

    @Test
    void testMaskFile() {
        assertNull(FileUtils.IsMaskFileName(null))
        assertFalse(FileUtils.IsMaskFileName('/tmp/test.getl/test123_a.txt'))
        assertTrue(FileUtils.IsMaskFileName('*.txt'))
        assertTrue(FileUtils.IsMaskFileName('/tmp/test.getl/test???_*.txt'))
    }

    @Test
    void testPathFromFile() {
        assertNull(FileUtils.PathFromFile(null))

        def p = FileUtils.ConvertToDefaultOSPath(TFS.systemPath)
        def f = new File("$p/test_pathfromfile.txt")
        f.deleteOnExit()
        f.text = 'test'

        assertEquals(p, FileUtils.PathFromFile("$p/test_pathfromfile.txt"))
        assertEquals(p, FileUtils.PathFromFile("$p/test_*.txt"))

        assertEquals('/', FileUtils.PathFromFile('resource:/getl-errors.en.properties', true, true))
        assertEquals('/utils', FileUtils.PathFromFile('resource:/utils/comments.sql', true, true))

        def r = new File(FileUtils.ResourceFileName('resource:/utils/comments.sql'))
        assertEquals(r.parent, FileUtils.PathFromFile('resource:/utils/comments.sql'))
    }

    @Test
    void testFileName() {
        assertNull(FileUtils.FileName(null))

        def p = FileUtils.ConvertToDefaultOSPath(TFS.systemPath)
        def f = new File("$p/test_pathfromfile.txt")
        f.deleteOnExit()
        f.text = 'test'

        assertEquals('test_pathfromfile.txt', FileUtils.FileName("$p/test_pathfromfile.txt"))
        assertEquals('test_*.txt', FileUtils.FileName("$p/test_*.txt"))

        assertEquals('getl-errors.en.properties', FileUtils.FileName('resource:/getl-errors.en.properties'))
        assertEquals('comments.sql', FileUtils.FileName('resource:/utils/comments.sql'))
    }

    @Test
    void testIsLockFileForRead() {
        assertNull(FileUtils.IsLockFileForRead(null))

        def fileName = "${TFS.systemPath}/test_lock.txt"
        def f = new File(fileName)
        f.deleteOnExit()
        f.text = 'test'

        def lf = FileUtils.LockFile(fileName, 'rw', false)
        assertTrue(FileUtils.IsLockFileForRead(fileName))
        lf.release()
        lf.channel().close()

        assertFalse(FileUtils.IsLockFileForRead(fileName))
    }

    @Test
    void testConvertText() {
        def original_text = new StringReader('Converted text 12345 test validation')
        def processed_text = new StringWriter()
        def rules = [
                [type: 'REGEXPR', old: 'text.*test', new: 'text-test'],
                [type: 'REPLACE', old: 'e', new: '[E]']
        ]
        FileUtils.ConvertText(original_text, processed_text, rules, null, null)
        assertEquals('Conv[E]rt[E]d t[E]xt-t[E]st validation\n', processed_text.toString())
    }

    @Test
    void testRun() {
        def f = new File("${TFS.systemPath}/check_run.groovy")
        f.deleteOnExit()
        f.text = '''
String time() { new java.sql.Timestamp(new Date().time).toString() + ' => ' }

println time() + 'start'
(1..5).each { 
    println time() + "number $it"
    sleep(700) 
}
println time() + 'finish' '''

        def codePage = (Config.isWindows())?'cp866':'utf-8'
        def cmd = FileUtils.TransformFilePath((Config.isWindows())?'{GROOVY_HOME}\\bin\\groovy.bat check_run.groovy':'{GROOVY_HOME}/bin/groovy check_run.groovy')

        def outConsole = new StringBuilder()
        def outErrors = new StringBuilder()
        def curConsole = 0
        def exitCode = FileUtils.Run(cmd, TFS.systemPath, codePage, outConsole, outErrors, 100) {
            if (outConsole.length() > curConsole) {
                println outConsole.substring(curConsole, outConsole.length() - 1) + ' >>> ' + new Timestamp(new Date().time)
                curConsole = outConsole.length()
            }
        }

        if (exitCode != 0)
            println outErrors.toString()

        assertEquals(0, exitCode)
        assertEquals(7, outConsole.readLines().size())
        assertEquals(0, outErrors.length())

        f.delete()
    }

    @Test
    void testZip() {
        def fileName = "${TFS.systemPath}/zip.${FileUtils.UniqueFileName()}"
        def zipName = fileName + '.zip'
        fileName += '.txt'
        def psw = 'TEST GETL ZIP'
        def text = 'test zip archive'

        def file = new File(fileName)
        file.deleteOnExit()
        file.text = text

        new File(zipName).deleteOnExit()
        FileUtils.CompressToZip(zipName, "${TFS.systemPath}/zip.*.txt",
                [compressionMethod: 'DEFLATE', compressionLevel: 'MAXIMUM', encryptFiles: true, encryptionMethod: 'AES',
                 aesKeyStrength: 'KEY_STRENGTH_256', password: psw])

        assertTrue(FileUtils.ExistsFile(zipName))
        assertTrue(FileUtils.DeleteFile(fileName))

        def extFileName = "${TFS.systemPath}/zip.files/zip.${FileUtils.UniqueFileName()}.txt"
        FileUtils.ValidFilePath(extFileName, true)
        def extFile = new File(extFileName)
        extFile.deleteOnExit()
        extFile.text = text
        FileUtils.CompressToZip(zipName, "${TFS.systemPath}/zip.files/zip.*.txt",
                [compressionMethod: 'DEFLATE', compressionLevel: 'MAXIMUM', encryptFiles: true, encryptionMethod: 'AES',
                 aesKeyStrength: 'KEY_STRENGTH_256', password: psw, rootDir: 'zip.files'])
        assertTrue(FileUtils.DeleteFile(extFileName))

        FileUtils.UnzipFile(zipName, TFS.systemPath, psw)
        try {
            assertTrue(FileUtils.ExistsFile(fileName))
            assertEquals(text, new File(fileName).text)
            assertTrue(FileUtils.ExistsFile(extFileName))
            assertEquals(text, new File(extFileName).text)
        }
        finally {
            FileUtils.DeleteFile(zipName)
            FileUtils.DeleteFile(fileName)
            FileUtils.DeleteFile(extFileName)
        }

        def zipPath = TFS.systemPath + '/test-zip'
        FileUtils.UnzipFile('resource:/reference/zip/test.zip', zipPath, null, 'cp866')
        try {
            assertEquals('12345', new File("$zipPath/тест.txt").text)
            assertEquals('12345', new File("$zipPath/1/тест.txt").text)
            assertEquals('12345', new File("$zipPath/1/2/тест.txt").text)

            FileUtils.CompressToZip(zipName, zipPath + '/*')
            FileUtils.DeleteFolder(zipPath, true)

            FileUtils.UnzipFile(zipName, zipPath)
            assertEquals('12345', new File("$zipPath/тест.txt").text)
            assertEquals('12345', new File("$zipPath/1/тест.txt").text)
            assertEquals('12345', new File("$zipPath/1/2/тест.txt").text)
        }
        finally {
            FileUtils.DeleteFolder(zipPath, true, false)
            new File(zipName).delete()
        }
    }

    @Test
    void testParseArguments() {
        assertEquals(['1', '2', '3'], FileUtils.ParseArguments('1 2 3'))
        assertEquals(['1', '2', '3'], FileUtils.ParseArguments('1  2  3'))
        assertEquals(['1', '2', '3', '4', '5'], FileUtils.ParseArguments('"1" 2 3 4 5'))
        assertEquals(['1', '2', '3', '4', '5'], FileUtils.ParseArguments('1 2 "3" 4 5'))
        assertEquals(['1', '2', '3', '4', '5'], FileUtils.ParseArguments('1 2 3 4 "5"'))
        assertEquals(['1 2 3', '4', '5'], FileUtils.ParseArguments('"1 2 3" 4 5'))
        assertEquals(['1', '2 3 4', '5'], FileUtils.ParseArguments('1 "2 3 4" 5'))
        assertEquals(['1', '2', '3 4 5'], FileUtils.ParseArguments('1 2 "3 4 5"'))
        assertEquals(['1', '2', 'message= 3  4  5 '], FileUtils.ParseArguments('1 2 "message= 3  4  5 "'))
        assertEquals(['123 "456" 789'], FileUtils.ParseArguments('"123 \\"456\\" 789"'))
        assertEquals(['"123 "456" 789"'], FileUtils.ParseArguments('"\\"123 \\"456\\" 789\\""'))
        shouldFail { FileUtils.ParseArguments('"1 2 3 4 5') }
        shouldFail { FileUtils.ParseArguments('1 2 "3 4 5') }
        shouldFail { FileUtils.ParseArguments('1 2 3 4 5"') }
        shouldFail { FileUtils.ParseArguments('"1 2 "3 4 5"') }

        def args = FileUtils.FileFromResources('/utils/parse_args.txt').text
        def res = FileUtils.FileFromResources('/utils/parse_args_res.txt').readLines()
        assertEquals(res, FileUtils.ParseArguments(args))

        /*def pb = new ProcessBuilder(args)
        pb.redirectOutput(new File('d:\\send\\test_cmd.txt'))
        pb.directory(new File('E:\\getl\\idea\\getl.vertica\\src\\test\\resources\\repository'))
        def p = pb.start()
        p.waitFor()*/
    }

    @Test
    void testClassLoaser() {
        shouldFail { FileUtils.ClassLoaderFromPath('tests/test.jar') }

        if (FileUtils.ExistsFile('tests/xero/demo.jar')) {
            def classLoader = FileUtils.ClassLoaderFromPath('tests/xero/demo.jar')
            def url = classLoader.getResource('xero.conf')
            assertNotNull(url)
            assertNotNull(url.text)

            classLoader = FileUtils.ClassLoaderFromPath('tests/xero')
            url = classLoader.getResource('xero.conf')
            assertNotNull(url)
            assertNotNull(url.text)

            classLoader = FileUtils.ClassLoaderFromPath('tests/xero/*.jar')
            url = classLoader.getResource('xero.conf')
            assertNotNull(url)
            assertNotNull(url.text)
        }
    }

    @Test
    void testFindParentPath() {
        assertNotNull(FileUtils.FindParentPath('.','getl\\.'))
        assertTrue(FileUtils.ExistsFile(FileUtils.FindParentPath('.', 'getl\\.') + 'getl/src'))
    }

    @Test
    void testReadFileFromResource() {
        def file1 = FileUtils.FileFromResources('/fileutils/file.txt')
        assertEquals('1234567890', file1.text)

        def file2 = FileUtils.FileFromResources('fileutils/file.txt', null, this.getClass().classLoader)
        assertEquals('1234567890', file2.text)
        assertFalse(file1 == file2)

        def file3 = FileUtils.FileFromResources('/fileutils/file.txt')
        assertEquals('1234567890', file3.text)
        assertEquals(file1, file3)

        assertNull(FileUtils.FileFromResources('/fileutils/file_none.txt'))
    }

    @Test
    void testParseFileName() {
        def resFileName = 'resource:/fileutils/file.txt'
        assertTrue(FileUtils.IsResourceFileName(resFileName))
        assertFalse(FileUtils.IsRepositoryFileName(resFileName))

        def repFileName = 'repository:/file.txt'
        assertFalse(FileUtils.IsResourceFileName(repFileName, false))
        assertTrue(FileUtils.IsResourceFileName(repFileName))
        assertTrue(FileUtils.IsRepositoryFileName(repFileName))

        def fileName1 = FileUtils.ResourceFileName(resFileName)
        assertTrue(new File(fileName1).exists())

        def fileName2 = FileUtils.ResourceFileName(fileName1)
        assertEquals(fileName1, fileName2)

        shouldFail { FileUtils.ResourceFileName(repFileName) }
        Getl.Dsl {getl ->
            shouldFail { FileUtils.ResourceFileName(repFileName, getl) }
            repositoryStorageManager.storagePath = 'resource:/fileutils'
            def fileName3 = FileUtils.ResourceFileName(repFileName, getl)
            assertTrue(new File(fileName3).exists())
        }
    }

    @Test
    void testFileMaskExpression() {
        def list = [
                'dir1-1/file*.ext': 'dir1\\-1/file.*[.]ext',
                'dir?-?\\file+1.*': 'dir.\\-.\\\\file\\+1[.].*',
                '%file*^.???': '[%]file.*[^][.]...'
        ]

        list.each { mask, rule ->
            assertEquals("$mask: $rule".toString(), rule, FileUtils.FileMaskToMathExpression(mask))
        }
    }

    @Test
    void testSizeBytes() {
        assertEquals('1000.0 bytes', FileUtils.SizeBytes(1000))
        assertEquals('1.2 KB', FileUtils.SizeBytes(1024L + 200))
        assertEquals('1.1 MB', FileUtils.SizeBytes(1024L * 1024 + 105000))
        assertEquals('1.0 GB', FileUtils.SizeBytes(1024L * 1024 * 1024 + 1024L * 1024))
        assertEquals('1.0 TB', FileUtils.SizeBytes(1024L * 1024 * 1024 * 1024 + 1024L * 1024 * 1024))
        assertEquals('1025.0 TB', FileUtils.SizeBytes(1024L * 1024 * 1024 * 1024 * 1024 + 1024L * 1024 * 1024 * 1024))
    }

    @Test
    void testAvgSpeed() {
        assertEquals('1 KB/sec', FileUtils.AvgSpeed(1024, 1000))
    }

    @Test
    void testRelativePathFromFile() {
        assertEquals('c:\\dir', FileUtils.RelativePathFromFile('c:\\dir\\file.ext'))
        assertEquals('c:\\dir', FileUtils.RelativePathFromFile('c:\\dir\\file.ext', '\\'))
        assertEquals('c:/dir', FileUtils.RelativePathFromFile('c:\\dir\\file.ext', true))
        assertEquals('.', FileUtils.RelativePathFromFile('c:\\dir\\file.ext', '/'))
        assertEquals('.', FileUtils.RelativePathFromFile('file.ext'))
    }

    @Test
    void testLockFile() {
        Getl.Dsl(this) {
            def counter = new SynchronizeObject()
            thread {
                useList (1..100)
                abortOnError = true
                def file = new File("${TFS.systemPath}/${FileUtils.UniqueFileName()}")
                file.deleteOnExit()
                def fileName = file.path
                run(50) {
                    def f = new File(fileName)
                    FileUtils.LockFile(f) {
                        if (!f.exists()) {
                            f.text = '12345'
                            counter.nextCount()
                        }
                        else
                            assertEquals('12345', f.text)
                    }
                }
            }
            assertEquals(1, counter.count)
            assertTrue(FileUtils.fileLockManager.isEmpty())
        }
    }

    @Test
    void testPrepareDirPath() {
        assertEquals('c:\\test', FileUtils.PrepareDirPath('c:\\test', false))
        assertEquals('c:\\test', FileUtils.PrepareDirPath('c:\\test\\', false))

        assertEquals('/home/test', FileUtils.PrepareDirPath('/home/test', true))
        assertEquals('/home/test', FileUtils.PrepareDirPath('/home/test/', true))
    }

    @Test
    void testAddToPath() {
        assertEquals('/child', FileUtils.AddToPath(null, 'child'))
        assertEquals('/child', FileUtils.AddToPath(null, '/child'))
        assertEquals('/child', FileUtils.AddToPath('.', 'child'))
        assertEquals('/child', FileUtils.AddToPath('.', '/child'))
        assertEquals('/main/child', FileUtils.AddToPath('/main', 'child'))
        assertEquals('/main/child', FileUtils.AddToPath('/main', '/child'))
        assertEquals('/main/child', FileUtils.AddToPath('/main/', 'child'))
        assertEquals('/main/child', FileUtils.AddToPath('/main/', '/child'))
    }

    @Test
    void testJarPath() {
        assertTrue(new File(FileUtils.JarPath(Getl)).exists())
    }
}