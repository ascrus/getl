package getl.lang

import getl.data.Field
import getl.exception.ExceptionGETL
import getl.utils.FileUtils
import org.junit.BeforeClass
import org.junit.Test

/**
Create config file "tests/lang/copier.groovy" in projection dir as syntax:

source {
 server = '<sftp host>'
 login = '<ssh user>'
 password = '<password>'
 rootPath = '<source directory of files>'
 hostKey = '<rsa public key>'
}

dest {
 server = '<sftp host>'
 login = '<ssh user>'
 password = '<password>'
 rootPath = '<source directory of files>'
 hostKey = '<rsa public key>'
}
*/

class CopyTest extends getl.test.GetlTest {
    @BeforeClass
    static void CleanGetl() {
        Getl.CleanGetl()
    }

    @Override
    boolean allowTests() { FileUtils.ExistsFile('tests/lang/copier.groovy') }

    final def configFile = 'tests/lang/copier.groovy'

    @Test
    void testCopy() {
        Getl.Dsl(this) {
            configuration {
                load(this.configFile)
            }

            embeddedTable('history', true) {
                tableName = 'history'
            }

            sftp('source', true) {
                useConfig 'source'
                useStory embeddedTable('history')
            }

            files('dest', true) {
                useConfig 'dest'
            }

            fileCopier(sftp('source'), files('dest')) {
                useSourcePath {
                    mask = 'M2000_{region}_{m2000}/neexport_{date}/{bs_group}/A{bs_date}00+{timezone_start}-{finish_hour}00+{timezone_finish}_{bs}.xml.gz'
                    variable('region') { format = 'CN|DV|KV|MO|NW|SB|UR|VL'}
                    variable('date') { type = Field.dateFieldType; format = 'yyyyMMdd' }
                    variable('bs_date') { type = Field.datetimeFieldType; format = 'yyyyMMdd.HH' }
                    variable('timezone_start') { type = Field.integerFieldType; length = 4 }
                    variable('timezone_finish') { type = Field.integerFieldType; length = 4 }
                    variable('finish_hour') { type = Field.integerFieldType; length = 2 }
                }

                useDestinationPath {
                    mask = 'm2000/{region}/{date}/{bs_date}'
                    variable('date') { type = Field.dateFieldType; format = 'yyyyMMdd' }
                    variable('bs_date') { type = Field.datetimeFieldType; format = 'HH-mm' }
                }

                useRenamePath {
                    mask = '{filenameonly}.{filedate}.{fileextonly}'
                }

                inMemoryMode = true

                /*source.buildList(path: sourcePath, recursive: true)
                source.fileList.eachRow {
                    println it.filename + ': ' + it.filedate.toString()
                    it.filenameonly = FileUtils.ExcludeFileExtension(it.filename)
                    it.fileextonly = FileUtils.FileExtension(it.filename)
                    println '  ' + renamePath.generateFileName(it)
                }*/

                retryCount = 3
                copyOrder = ['bs_date', 'region']

                scriptOfSourceOnStart = 'ls'
                scriptOfDestinationOnStart = 'dir'

                scriptOfSourceOnComplete = 'ls'
                scriptOfDestinationOnComplete = 'dir'

                beforeCopyFile { s, d -> println "Start copy file $s to file $d" }
                afterCopyFile { s, d -> println "Finish copy file $s to file $d" }
            }
        }
    }
}