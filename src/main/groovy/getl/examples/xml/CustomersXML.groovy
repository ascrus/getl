/**
 * This example shows how to read data from a XML file and write it into two h2 tables as master-detail.
 * P.S. This is script used from h2 installation script.
 */

package getl.examples.xml

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

// Define xml file
xml('customers', true) { xml ->
    rootNode = 'customer'
    defaultAccessMethod = DEFAULT_NODE_ACCESS // Fields values are stored as node value

    field('id') { type = integerFieldType }
    field('name')
    field('customer_type') { alias = '@'} // Customer value are stored as attribute value
    field('phones') { type = objectFieldType } // Phones are stored as array list values and will be manual parsing

    // Write xml text to temporary file
    textFile { file ->
        // This file will be storage in temp directory and have randomize file name.
        temporaryFile = true
        // Append text to buffer
        text '''<?xml version="1.0" encoding="UTF-8"?>
<customers>
	<customer customer_type="wholesale">
	    <id>1</id>
	    <name>Customer 1</name>
		<phones>
			<phone>+7 (001) 100-00-01</phone>
			<phone>+7 (001) 100-00-02</phone>
			<phone>+7 (001) 100-00-03</phone>
		</phones>
	</customer>
	<customer customer_type="retail">
	    <id>2</id>
	    <name>Customer 2</name>
		<phones>
			<phone>+7 (111) 111-00-11</phone>
			<phone>+7 (111) 111-00-12</phone>
		</phones>
	</customer>
	<customer customer_type="retail">
	    <id>3</id>
	    <name>Customer 3</name>
		<phones>
			<phone>+7 (222) 222-00-11</phone>
			<phone>+7 (222) 222-00-12</phone>
		</phones>
	</customer>
</customers>
'''
        // Write buffer to file
        write()

        // Set file name to xml
        xml.fileName = fileName
    }
}
