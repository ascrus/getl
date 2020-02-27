# Create Groovy class
Getl Dsl script is a extension of Groovy language. Create a new Groovy class in your IDE and modify its contents:
```groovy
package <class package>

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

...your code...
```
# The principles of the Getl Dsl script
The BaseScript modifier indicates to Groove that the script should be executed through Getl. When the script is executed, the variable "main" is automatically created, which is a reference to Getl himself, his methods and properties. 

The script code sees all the objects, methods and properties of the Getl language and there is no need to explicitly access its variable:
```groovy
package demo

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

// Call Getl log command
logInfo 'Hello world!' 
```
But in case of access to Getl from the code of options of objects, an explicit reference to it through the variable "main" will be required:
```groovy
package demo

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

oracleTable {
  readOpts {
    // Call Getl log command
    main.logInfo 'Hello world!' 
  }
}
```

# Call another Getl script
Use function "runGroovyClass" to call another Getl script from your project:
```groovy
package demo

import groovy.transform.BaseScript
import getl.lang.Getl

@BaseScript Getl main

runGroovyClass demo.Script2
```

If you want the called script to be called within the application no more than once, add the value "true" as the second parameter:
```groovy
runGroovyClass demo.Script2, true
```
This will allow you to write scripts for defining objects and guarantee that from any place in the application code they will be called only once and will not generate an error of re-creating existing objects.

If you want to pass a set of parameters to the called script, use the definition of parameters in the called script using "@Field":
```groovy
package demo

import groovy.transform.BaseScript
import groovy.transform.Field
import getl.lang.Getl

@BaseScript Getl main

// Set default 0 to parameter
@Field int param1 = 0 

@Field String param2
// Check that the parameter is passed
assert param2 != null 

@Field Map param3
assert param3 != null

logInfo "param1=$param1; param2=$param2; param3=$param3"
```

When calling parameterized scripts, pass the values in the last parameter of the function:
```groovy
runGroovyClass demo.Script2, { 
 param1 = 100
 param2 = 'test'
 param3 {
   node1 = 1
   node2 = '2'
   node3 = [1,2,3]
 }
}
runGroovyClass demo.Script2, false, { 
 param2 = 'test' 
 param3 {
   node1 = 1
   node2 = '2'
   node3 = [1,2,3]
 }
}

// Assert will work because "param2" is not passed
runGroovyClass demo.Script2, false, { param1 = 100 }
```

Using parameters in Getl scripts allows you to develop reusable templates to automate complex logic. An error will occur if a parameter is passed that is not defined in the called script. When declaring parameters, use assertions to ensure that all the necessary values are passed to the script.

# Script procedures
To execute additional logic for initializing the script, checking the passed parameters, processing errors or finalizing its work, it is enough to describe the procedures with the names "init", "check", "error" and "done" inside the script:
```groovy
package demo

import groovy.transform.BaseScript
import groovy.transform.Field
import getl.lang.Getl

@BaseScript Getl main

@Field int param1

/* Called before setting values into the script fields  
   from the configuration files or variables wich were 
   specified on the command line */
void init() {
  /* Read the configuration file from the resources and 
     set the parameters from section “init” to the script */
  configuration {
    loadFile 'resource:/myconfig.groovy'
    readFields 'init'
  }
}

/* Called after the values of the script fields have been set */
void check() {
  assert param1 > 0
}

/* Called if an error occurred while executing the script logic */
void error(Exception e) {
  ... code for handling script errors
}

/* Called after the script has been executed or 
   completed with an error */
void done() {
  ... resource release code after the script stops working
}

... script code
```