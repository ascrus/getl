package getl.examples.utils

import getl.data.Field
import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

filePath {
    mask = '/{group}/{prefix}_{date}_{suffix}.{ext}'
    variable('date') {
        type = Field.datetimeFieldType
        format = 'yyyy-MM-dd_HH-mm-ss'
    }
    compile()
    logInfo "Reqular expression: $maskPath"
    logInfo "Parse variables: $vars"

    def fileVars = analizeFile('/group-1/beg_2019-01-31_10-59-30_end.txt')
    logInfo "Variables: $fileVars"
}