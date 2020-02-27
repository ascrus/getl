To use the DSL script inside the Groove class code, use the static Dsl method:
```groovy
import getl.lang.Getl

class MyClass {
  public final def countRows= 100

  void myMethod() {
    Getl.Dsl(this) {
      embeddedTable('table1', true) {
        tableName = 'table1'
        field('id') { type = integerFieldType; isKey = true }
        field('name') { length = 50; isNull = false }
        create(ifNotExists: true)
        rowsTo {
          writeRow { add ->
            (1..this.countRows).each { num ->
              add id: num, name: "name $num"
            }
          }
        }
      }
    }
  }
}
```
P.S. To access the class object inside the Dsl code, pass the object reference to the Dsl () function and access its properties and methods through this.

Similarly, you can use the capabilities of the Dsl language for unit tests:
```groovy
import getl.lang.Getl
import groovy.transform.InheritConstructors
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@InheritConstructors
@RunWith(JUnit4.class)
class MyTest extends GroovyTestCase {
  @Test
  void testDsl() {
    def obj = new MyClass()
    obj.myMethod()
    Getl.Dsl(this) {
      embeddedTable('table1') {
        readOpts { order = ['id'] }
        int i = 0
        eachRow { row ->
          i++
          assertEquals(i, row.id)
          assertEquals("name $i", row.name)
        }
        assertEquals(obj.countRows, i)
      }
    }
  }
}
```