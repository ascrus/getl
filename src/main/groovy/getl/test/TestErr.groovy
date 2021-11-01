package getl.test

import getl.utils.FileUtils

class TestErr {

    static void main(def args) {
        try {
            println new File(FileUtils.TransformFilePath('{EASYLOADER_TEST}/archives')).text
        }
        catch (Exception e) {
            println e.message
        }
    }
}