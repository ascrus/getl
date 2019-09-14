package getl.lang

import getl.data.Field
import getl.utils.FileUtils
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
    @Override
    boolean allowTests() { FileUtils.ExistsFile('tests/lang/copier.groovy') }

    @Test
    void testCopy() {
        Getl.Dsl {
            configuration {
                load('tests/lang/copier.groovy')
            }

            sftp('source', true) {
                server = configContent.source.server

                login = configContent.source.login
                password = configContent.source.password
                rootPath = configContent.source.rootPath
                hostKey = configContent.source.hostKey
            }

            sftp('dest', true) {
                server = configContent.dest.server

                login = configContent.dest.login
                password = configContent.dest.password
                rootPath = configContent.dest.rootPath
                hostKey = configContent.dest.hostKey
            }

            fileCopier(sftp('source'), sftp('dest')) {
                useSourcePath {
                    mask = 'M2000_{region}_{m2000}/neexport_{date}/{bs_group}/A{bs_date}00+{timezone_start}-{finish_hour}00+{timezone_finish}_{bs}.xml.gz'
                    variable('region') { format = '(CN|FE|KV|MO|NW|SB|UR|VL)'}
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

                directoryConcurrencyNestingLevel = 2
                countThreadWhenBuildSourceList = 4
                retryCount = 3
                copyOrder = ['bs_date', 'region']
            }
        }
    }
}