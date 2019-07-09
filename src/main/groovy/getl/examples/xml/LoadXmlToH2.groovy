/**
 * This example shows how to read data from a XML file and write it into two h2 tables as master-detail.
 * P.S. This is script used from h2 installation script.
 */

package getl.examples.xml

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

// Define xml file
xml('customers') { xml ->
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

// Copy customers rows from xml file to h2 tables customers and customers_phones
copyRows(xml('customers'), embeddedTable('customers')) {
    bulkLoad = true

    // Adding an write to the child table customers_phones
    childs('customers.phones', embeddedTable('customers.phones')) {
        // Processing the child structure phones
        processRow { addPhone, row ->
            // Copying phones array to the writer in h2 table phones customers
            row.phones?.each { phone ->
                addPhone customer_id: row.id, phone: phone?.text()
            }
        }
        childDone { logInfo "${dataset.updateRows} customer phones loaded" }
    }
    doneFlow { logInfo "${destination.updateRows} customers loaded" }
}

assert embeddedTable('customers').countRow() == 3
assert embeddedTable('customers.phones').countRow() == 7