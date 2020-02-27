# Running as Groovy script
Getl script is an ordinary Groovy script and can be launched using Groovy installed from the command line:
```
groovy demo/Script1.groovy
```
# Running a script using the Getl class
To run Getl scripts, it is recommended to use the "getl.lang.Getl" launcher, which correctly initializes the application launch, sets up the configuration and logging environment:
```
java -cp "<getl jars dir>/*";"<your jar>" getl.lang.Getl runclass=demo.Script1
```
With this method of calling scripts, the source code is not required, the compiled Java byte code is executed, it is possible to work with resource files inside the jar file. To generate the jar file of your application, use gradle.

If you want to pass parameters to the script that is running, describe the parameters in it through the "@Field" modifier and specify the values for the parameters on the command line:
```
java -cp "<getl jars dir>/*";"<your jar>" getl.lang.Getl runclass=demo.Script1 vars.param1=test
```
Warning: the passed parameters will be converted to the type that is declared for the field in the script. To avoid errors, specify explicit field types instead of "def"!