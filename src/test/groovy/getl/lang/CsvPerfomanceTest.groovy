package getl.lang

import getl.csv.CSVDriverTest

class CsvPerfomanceTest {
    static void main(def args) {
        def t = new CSVDriverTest()
        t.testPerfomanceLinux()
    }
}
