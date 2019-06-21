/**
 * This example shows how to read data from a XML file and write it into two csv files master-detail.
 */

package getl.examples.xml

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

// Define json file
xml('customers') { xml ->
    rootNode = 'customer'
    defaultAccessMethod = DEFAULT_NODE_ACCESS

    field('id') { type = integerFieldType }
    field('name')
    field('customer_type') { alias = '@'}
    field('phones') { type = objectFieldType }

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

        // Set file name to json
        xml.fileName = fileName
    }
}

// Define csv temporary file for customers data
csvTemp('customers') {
    // Used the json fields minus the array phones
    field = xml('customers').field
    removeField'phones'
    resetFieldToDefault()
}

// Define csv temporary file for customers phones data
csvTemp('customers.phones') {
    // Adding the customer identification field and him phone
    field('customer_id') { type = integerFieldType }
    field('phone')
}

// Generate writer the phones customers to temporary file
rowsTo(csvTemp('customers.phones')) { csv_phones  ->
    // Write processing
    process { addPhone -> // Writer object
        // Generate copy the customers from json file to temporary file
        copyRows(xml('customers'), csvTemp('customers')) { xml, csv ->
            // Copy processing
            process { source, dest ->
                // Copying phones array to the writer in temporary file phones customers
                source.phones?.each { phone ->
                    addPhone customer_id: source.id, phone: phone?.text()
                }
            }
        }
    }
}

println 'Customers:'
rowProcess(csvTemp('customers')) {
    process { println it}
}

println 'Customers phones:'
rowProcess(csvTemp('customers.phones')) {
    process { println it }
}