package getl.lang

import getl.utils.Config

class DslApplication extends Getl {
    static void main(def args) {
        Application(this, args)
    }

    String field1
    Integer field2

    void init() {
        Getl.Dsl(this) {
            configContent.init = true
        }
    }

    void check() {
        Getl.Dsl(this) {
            testCase {
                assertEquals('test application', this.field1)
                assertEquals(100, this.field2)
            }
            configContent.check = true
        }
    }

    void done() {
        Getl.Dsl(this) {
            configContent.done = true
        }
    }

    @Override
    Object run() {
        Getl.Dsl(this) {
            configContent.run = true
        }
    }
}