/**
 * This example shows how to read data from a JSON file and write it into two csv temporary files as master-detail.
 */

package getl.examples.json

import groovy.transform.BaseScript

@BaseScript getl.lang.Getl getl

// Define json file
json('customers') { json ->
    rootNode = 'customers'

    field('id') { type = integerFieldType }
    field('name')
    field('phones') { type = objectFieldType }


    // Write json text to temporary file
    textFile { file ->
        // This file will be storage in temp directory and have randomize file name.
        temporaryFile = true
        // Append text to buffer
        text '''
{
    "customers": [
        {
            "id":1,
            "name":"Customer 1",
            "phones": [
                { "phone": "+7 (001) 100-00-01" },
                { "phone": "+7 (001) 100-00-02" },
                { "phone": "+7 (001) 100-00-03" }
            ]
        },
        {
            "id":2,
            "name":"Customer 2",
            "phones": [
                { "phone": "+7 (001) 200-00-01" },
                { "phone": "+7 (001) 200-00-02" },
                { "phone": "+7 (001) 200-00-03" }
            ]
        },
        {
            "id":3,
            "name":"Customer 3",
            "phones": [
                { "phone": "+7 (001) 300-00-01" },
                { "phone": "+7 (001) 300-00-02" },
                { "phone": "+7 (001) 300-00-03" }
            ]
        }
    ]
}
'''
        // Write buffer to file
        write()

        // Set file name to json
        json.fileName = fileName
    }
}

// Define csv temporary file for customers data
csvTemp('customers') {
    // Used the json fields minus the array phones
    field = json('customers').field
    removeField'phones'
}

// Define csv temporary file for customers phones data
csvTemp('customers.phones') {
    // Adding the customer identification field and him phone
    field('customer_id') { type = integerFieldType }
    field('phone')
}

// Generate writer the phones customers to temporary file
copyRows(json('customers'), csvTemp('customers')) {
    // Adding an write to the child table customers_phones
    childs('customers.phones', csvTemp('customers.phones')) {
        // Processing the child structure phones
        processRow { addPhone, row ->
            // Copying phones array to the writer in h2 table phones customers
            row.phones?.each { phone ->
                addPhone customer_id: row.id, phone: phone.phone
            }
        }
        childDone { logInfo "${dataset.updateRows} customer phones loaded" }
    }

    doneFlow { logInfo "${destination.updateRows} customers loaded" }
}

println 'Customers:'
rowProcess(csvTemp('customers')) {
    process { println it}
}

println 'Customers phones:'
rowProcess(csvTemp('customers.phones')) {
    process { println it }
}