# Adding to the Gradle project
Write in the file build.gradle in section dependencies:
```gradle
compile(group: 'net.sourceforge.getl', name: 'getl', version:'4.1.0')
```

# Exception of unused modules
If you will not use the work with hdfs, Xero and SalesForce, then you can add an exception to these groups:
```gradle
compile(group: 'net.sourceforge.getl', name: 'getl', version:'4.1.0) {
    exclude group: "com.github.xeroapi"
    exclude group: "org.apache.hadoop"
    exclude group: "com.force.api"
}
```
# Completion of setup
Update gradle project in your IDE.