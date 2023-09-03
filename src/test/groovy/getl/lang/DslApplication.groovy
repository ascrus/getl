package getl.lang

class DslApplication extends Getl {
    static void main(def args) {
        Application(this, args)
    }

    public String field1
    public Integer field2

    void init() {
        configContent.init = true
    }

    void check() {
        testCase {
            assert'test application' == this.field1
            assert 100 == this.field2
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