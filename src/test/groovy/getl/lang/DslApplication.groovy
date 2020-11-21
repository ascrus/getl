package getl.lang

class DslApplication extends Getl {
    static void main(def args) {
        Application(this, args)
    }

    String field1
    Integer field2

    @Override
    protected  void initGetlParams() {
        super.initGetlParams()
        configContent.init = true
    }

    void check() {
        testCase {
            assertEquals('test application', this.field1)
            assertEquals(100, this.field2)
        }
        configContent.check = true
    }

    void done() {
        configContent.done = true
    }

    @Override
    Object run() {
        configContent.run = true
    }
}