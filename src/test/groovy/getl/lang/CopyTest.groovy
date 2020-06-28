package getl.lang


import getl.files.Manager
import getl.test.GetlDslTest
import getl.test.TestInit
import getl.test.TestRunner
import getl.utils.FileUtils
import getl.utils.Path
import getl.utils.StringUtils
import static getl.test.TestRunner.Dsl

import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

class CopyTest extends GetlDslTest {
    @Override
    Class<Getl> useInitClass() { TestInit }
    @Override
    Class<Getl> useGetlClass() { TestRunner }
    @Override
    protected boolean cleanGetlBeforeTest() { false }

    static final def debug = false
    static final def workPath = "${(debug)?"${System.getenv().GETL_TEST}/getl.test":FileUtils.SystemTempDir()}/copier"

    static final def sourcePathDir = "${workPath}/source"
    static final def destPathDir = "${workPath}/dest"

    static final def countFilePortions = 1024 // 10 chars *  1024 bytes = 10 kb

    @BeforeClass
    static void init() {
        if (FileUtils.ExistsFile(workPath, true)) {
            FileUtils.DeleteFolder(workPath, true)
        }
        FileUtils.ValidPath(workPath, !debug)
        FileUtils.ValidPath(sourcePathDir)

        Dsl(this) {
            files('source', true) {
                rootPath = this.sourcePathDir
                threadLevel = 1
                buildListThread = 2
            }
        }
    }

    protected void generateSource() {
        if (FileUtils.ExistsFile(sourcePathDir, true)) {
            FileUtils.DeleteFolder(sourcePathDir, true)
        }

        Dsl(this) {
            thread {
                useList(1..3)
                run(3) { region_num ->
                    FileUtils.ValidPath("${this.sourcePathDir}/region_$region_num")
                    (1..3).each { day_num ->
                        FileUtils.ValidPath("${this.sourcePathDir}/region_$region_num/2020-01-${StringUtils.AddLedZeroStr(day_num, 2)}")
                        ['AAA', 'BBB', 'CCC'].each { obj_name ->
                            (1..3).each { portion_num ->
                                def fc = textFile("${this.sourcePathDir}/region_$region_num/2020-01-${StringUtils.AddLedZeroStr(day_num, 2)}/region_${region_num}_object_${obj_name}.${StringUtils.AddLedZeroStr(portion_num, 4)}.dat") {
                                    writeln StringUtils.Replicate('0123456789', this.countFilePortions)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @AfterClass
    static void done() {
        if (!debug) {
            FileUtils.DeleteFolder(workPath, true)
        }
    }

    static final Path sourceMask = Dsl {
            filePath {
                mask = 'region_{region}/{date}/region_{region_num}_object_{name}.{num}.dat'

                variable('region') { type = integerFieldType; minimumLength = 1; maximumLength = 3 }
                variable('date') { type = dateFieldType; format = 'yyyy-MM-dd' }
                variable('region_num') { type = integerFieldType; minimumLength = 1; maximumLength = 3 }
                variable('name') { format = 'AAA|BBB|CCC' }
                variable('num') { type = integerFieldType; length = 4 }
            }
        } as Path

    static final Path destMask = Dsl {
        filePath {
            mask = '{date}/region_{region}/{name}'
            variable('date') { type = dateFieldType; format = 'yyyyMMdd' }
        }
    } as Path

    protected long copy(Manager src, Path srcMask, List<Manager> dst, Path dstMask, Path renameMask = null,
                        boolean delFiles = false, boolean delDirs = false, boolean inMemoryMode = true, boolean cacheStory = true) {
        long res = 0
        Dsl(this) {
            res = fileman.copier(src, dst) {
                sourcePath = srcMask
                destinationPath = dstMask

                if (renameMask != null)
                    renamePath = renameMask

                it.inMemoryMode = inMemoryMode

                if (cacheStory)
                    cacheFilePath = "${this.workPath}/filecopiercache"

                numberAttempts = 3
                timeAttempts = 2

                order = ['date', 'region', 'name']
                segmented = ['name']

                sourceBeforeScript = 'dir'
                destinationBeforeScript = 'dir'

                sourceAfterScript = 'dir'
                destinationAfterScript = 'dir'

                sourceErrorScript = 'dir'
                destinationErrorScript = 'dir'

                removeFiles = delFiles
                removeEmptyDirs = delDirs

                beforeCopyFile { s, d ->
                    assert s.region != null && s.date != null && s.region_num != null && s.name != null && s.num != null
                    assert d.region != null && d.date != null && d.region_num != null && d.name != null && d.num != null
                    assert (renameMask == null && s.filepath != d.filepath) || (renameMask != null && s.filepath == d.filepath)
                }
                afterCopyFile { s, d ->
                    assert s.region != null && s.date != null && s.region_num != null && s.name != null && s.num != null
                    assert d.region != null && d.date != null && d.region_num != null && d.name != null && d.num != null
                    assert (renameMask == null && s.filepath != d.filepath) || (renameMask != null && s.filepath == d.filepath)
                }
            }.countFiles
        }

        return res
    }

    @Test
    void testCopyWithSingle() {
        generateSource()

        Dsl(this) {
            files('single', true) {
                rootPath = "${this.destPathDir}/single"
                createRootPath = true
            }

            def historyTable = 'history_single'
            embeddedTable(historyTable, true) {
                tableName = historyTable
            }

            files('source') {
                useStory embeddedTable(historyTable)
                createStory = true
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-single.{date}.sql"
            }

            def countFiles = this.copy(files('source'), sourceMask, [files('single')] as List<Manager>,
                    destMask, null, false, false, false, false)
            testCase {
                assertEquals(81, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            countFiles = this.copy(files('source'), sourceMask, [files('single')] as List<Manager>, destMask)
            testCase {
                assertEquals(0, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            embeddedTable(historyTable) { truncate( )}
            files('source') { story = null }

            countFiles = this.copy(files('source'), sourceMask, [files('single')] as List<Manager>, destMask,
                    null, true)
            testCase {
                assertEquals(81, countFiles)
                assertEquals(0, embeddedTable(historyTable).countRow())
            }

            files('source') {
                def list = buildListFiles {
                    recursive = true
                    maskPath = this.sourceMask
                }
                testCase {
                    assertEquals(0, list.countRow())
                }
            }

            this.generateSource()
            countFiles = this.copy(files('source'), sourceMask, [files('single')] as List<Manager>, destMask,
                    null,true, true)
            files('source') {
                assertEquals(0, list().size())
            }
        }

        if (!debug)
            FileUtils.DeleteFolder("${this.destPathDir}/single", true)
    }

    @Test
    void testCopyWithMany() {
        generateSource()

        Dsl(this) {
            files('many.1', true) {
                rootPath = "${this.destPathDir}/many/1"
                createRootPath = true
            }

            files('many.2', true) {
                rootPath = "${this.destPathDir}/many/2"
                createRootPath = true
            }

            files('many.3', true) {
                rootPath = "${this.destPathDir}/many/3"
                createRootPath = true
            }

            def historyTable = 'history_many'
            embeddedTable(historyTable, true) {
                tableName = historyTable
            }

            files('source') {
                useStory embeddedTable(historyTable)
                createStory = true
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-many.{date}.sql"
            }

            def countFiles = this.copy(files('source'), sourceMask,
                    [files('many.1'), files('many.2'), files('many.3')] as List<Manager>, destMask)
            testCase {
                assertEquals(81, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            countFiles = this.copy(files('source'), sourceMask,
                    [files('many.1'), files('many.2'), files('many.3')] as List<Manager>, destMask)
            testCase {
                assertEquals(0, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }
        }

        if (!debug)
            FileUtils.DeleteFolder("$destPathDir/many", true)
    }

    @Test
    void testCopyWithRename() {
        generateSource()

        Dsl(this) {
            files('rename', true) {
                rootPath = "${this.destPathDir}/rename"
                createRootPath = true
            }

            files('source') {
                story = null
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-rename.{date}.sql"
            }

            def countFiles = this.copy(files('source'), sourceMask, [files('rename')] as List<Manager>,
                    filePath { mask = '.' },
                    filePath { mask = '{date}.{filenameonly}.{filedate}.{fileextonly}'
                        variable('date') { format = 'yyyy_MM_dd'}
                        variable('filedate') { format = 'yyyy_MM_dd-HH_mm_ss'}
                    }
            )
            testCase {
                assertEquals(81, countFiles)
            }

            files('rename') {
                def list = buildListFiles {
                    recursive = true
                    useMaskPath {
                        mask = 'region_{region}/{date}/{objdate}.region_{objregion}_object_{name}.{num}.{filetime}.dat'
                        variable('region') { type = integerFieldType; minimumLength = 1; maximumLength = 3 }
                        variable('date') { type = dateFieldType; format = 'yyyy-MM-dd' }
                        variable('objregion') { type = integerFieldType; minimumLength = 1; maximumLength = 3 }
                        variable('objdate') { type = dateFieldType; format = 'yyyy_MM_dd' }
                        variable('name') { format = 'AAA|BBB|CCC' }
                        variable('num') { type = integerFieldType; length = 4 }
                        variable('filetime') { type = datetimeFieldType;  format = 'yyyy_MM_dd-HH_mm_ss' }
                    }
                }
                testCase {
                    assertEquals(81, list.countRow())
                }
            }
        }

        if (!debug)
            FileUtils.DeleteFolder("${this.destPathDir}/rename", true)
    }

    @Test
    void testSimpleCopy() {
        Dsl(this) {
            def simplePath = "$workPath/simple"
            if (FileUtils.ExistsFile(simplePath, true))
                FileUtils.DeleteFolder(simplePath, true)

            FileUtils.ValidPath(simplePath)
            FileUtils.ValidPath("$simplePath/source")
            FileUtils.ValidPath("$simplePath/dest")
            try {
                (1..9).each { num ->
                    textFile("$simplePath/source/${num}.txt") {
                        write '1234567890'
                    }
                }
                sleep(1000)
                fileman.copier(files('in', true) { rootPath =  "$simplePath/source"; saveOriginalDate = true },
                        files('out', true) { rootPath = "$simplePath/dest"; saveOriginalDate = true }) {
                    useSourcePath { mask = '*.txt' }
                    useDestinationPath { mask = '.' }
                }

                def inList = files('in').buildListFiles { useMaskPath { mask = '*.txt'} }
                def outList = files('out').buildListFiles { useMaskPath { mask = '*.txt'} }

                assertEquals(9, outList.countRow())
                assertEquals(inList.rows(order: ['FILEPATH', 'FILENAME']), outList.rows(order: ['FILEPATH', 'FILENAME']))
            }
            finally {
                if (!debug)
                    FileUtils.DeleteFolder(simplePath, true)
            }
        }
    }

    @Test
    void testCleaner() {
        Dsl(this) {
            def historyTable = 'history_remove'
            embeddedTable(historyTable, true) {
                tableName = historyTable
            }

            this.generateSource()

            files('source') {
                useStory embeddedTable(historyTable)
                createStory = true
                if (this.debug) sqlHistoryFile = "${this.workPath}/h2-remove.{date}.sql"
            }

            def countFiles = fileman.cleaner(files('source')) {
                sourcePath = this.sourceMask
                cacheFilePath = "${this.workPath}/filecleancache"
            }.countFiles
            testCase {
                assertEquals(81, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            this.generateSource()
            countFiles = fileman.cleaner(files('source')) {
                sourcePath = this.sourceMask
                cacheFilePath = "${this.workPath}/filecleancache"
            }.countFiles
            testCase {
                assertEquals(0, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            countFiles = fileman.cleaner(files('source')) {
                sourcePath = this.sourceMask
                cacheFilePath = "${this.workPath}/filecleancache"
                onlyFromStory = true
            }.countFiles
            testCase {
                assertEquals(81, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            this.generateSource()
            countFiles = fileman.cleaner(files('source')) {
                sourcePath = this.sourceMask
                cacheFilePath = "${this.workPath}/filecleancache"
                ignoreStory = true
            }.countFiles
            testCase {
                assertEquals(81, countFiles)
                assertEquals(81, embeddedTable(historyTable).countRow())
            }

            files('source').story.truncate()
            this.generateSource()
            countFiles = fileman.cleaner(files('source')) {
                sourcePath = this.sourceMask
                cacheFilePath = "${this.workPath}/filecleancache"
                ignoreStory = true
            }.countFiles
            testCase {
                assertEquals(81, countFiles)
                assertEquals(0, embeddedTable(historyTable).countRow())
            }

            embeddedTable(historyTable).drop()
        }
    }
}