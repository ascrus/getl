/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.xero

import com.xero.api.JsonConfig
import com.xero.api.XeroClient
import getl.csv.CSVDataset
import getl.data.Dataset
import getl.data.Field
import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.utils.BoolUtils
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.Logs
import getl.utils.MapUtils
import groovy.transform.InheritConstructors
import org.codehaus.groovy.tools.RootLoader

/**
 * Xero connection driver
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class XeroDriver extends Driver {
    private XeroClient client

    XeroDriver() {
        methodParams.register('retrieveObjects', ['xeroObjectName', 'xeroListName'])
        methodParams.register('eachRow', ['modifiedAfter', 'where', 'orderBy', 'limit', 'includeArchived', 'filter'])
        methodParams.register('rows', ['modifiedAfter', 'where', 'orderBy', 'limit', 'includeArchived', 'filter'])
    }

    @Override
    List<Driver.Support> supported() { return [Driver.Support.EACHROW, Driver.Support.CONNECT] }

    @Override
    List<Driver.Operation> operations() { return [Driver.Operation.RETRIEVEFIELDS] }

    @Override
    boolean isConnected() { client != null }

    protected void saveToHistory(String content) {
        def con = connection as XeroConnection
        if (con.historyFile != null) {
            con.validHistoryFile()
            def f = new File(con.fileNameHistory).newWriter("utf-8", true)
            try {
                f.write("*** ${DateUtils.NowDateTime()}\n")
                f.write(content)
                f.write('\n\n')
            }
            finally {
                f.close()
            }
        }
    }

    @Override
    void connect() {
        XeroConnection con = connection as XeroConnection

        if (con.useResourceFile != null) {
            if (!FileUtils.ExistsFile(con.useResourceFile))
                throw new ExceptionGETL("Resource file \"${con.useResourceFile}\" not found!")
            saveToHistory("Loading jar file \"${con.useResourceFile}\"")
//            ClassLoader.systemClassLoader.addURL(new File(con.useResourceFile).toURI().toURL())
        }

        if (con.configInResource == null)
            throw new ExceptionGETL('Required parameter "configInResource"!')

        saveToHistory("Loading config \"${con.configInResource}\"")
        def config = new JsonConfig(con.configInResource)

        saveToHistory("Connecting to Xero API")
        client = new XeroClient(config)
    }

    @Override
    void disconnect() {
        saveToHistory("Disconnecting")
        client = null
    }

    public static final String xsdPath = 'XeroSchemas/v2.00'

    public static final List<Map> objects = [
            [objectName: 'Account', listName: 'Accounts', pk: ['AccountID']],

            [objectName: 'BankTransaction', listName: 'BankTransactions', pk: ['BankTransactionID']],
            [objectName: 'BankTransactionLineItem', listName: 'BankTransactions',
             pk: ['BankTransactionID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'BankTransaction', parentColumn: 'LineItems',
             parentOnly: ['BankTransactionID', 'UpdatedDateUTC']],

            [objectName: 'BankTransfer', listName: 'BankTransfers', pk: ['BankTransferID']],

            [objectName: 'BrandingTheme', listName: 'BrandingThemes', pk: ['BrandingThemeID']],

            [objectName: 'ContactGroup', listName: 'ContactGroups', pk: ['ContactGroupID']],

            [objectName: 'Contact', listName: 'Contacts', pk: ['ContactID']],
            [objectName: 'ContactAddress', listName: 'Contacts', pk: ['ContactID', 'Addresses.AddressType'],
             childClass: 'Address', parentName: 'Contact', parentColumn: 'Addresses',
             parentOnly: ['ContactID', 'UpdatedDateUTC']],
            [objectName: 'ContactPhone', listName: 'Contacts', pk: ['ContactID', 'Phones.PhoneType'],
             childClass: 'Phone', parentName: 'Contact', parentColumn: 'Phones',
             parentOnly: ['ContactID', 'UpdatedDateUTC']],
            [objectName: 'ContactGroupRel', listName: 'Contacts', pk: ['ContactID', 'ContactGroups.ContactGroupID'],
             childClass: 'ContactGroup', parentName: 'Contact', parentColumn: 'ContactGroups',
             parentOnly: ['ContactID', 'UpdatedDateUTC'], childOnly: ['ContactGroupID']],
            [objectName: 'ContactBalance', listName: 'Contacts', pk: ['ContactID'],
             childClass: 'Balances', parentName: 'Contact', parentColumn: 'Balances',
             parentOnly: ['ContactID', 'UpdatedDateUTC']],

            [objectName: 'CreditNote', listName: 'CreditNotes', pk: ['CreditNoteID']],
            [objectName: 'CreditNoteLineItem', listName: 'CreditNotes', pk: ['CreditNoteID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'CreditNote', parentColumn: 'LineItems',
             parentOnly: ['CreditNoteID', 'UpdatedDateUTC']],
            [objectName: 'CreditNoteAllocation', listName: 'CreditNotes', pk: ['CreditNoteID', 'Allocations.Invoice.InvoiceID', 'Allocations.Date'],
             childClass: 'Allocation', parentName: 'CreditNote', parentColumn: 'Allocations',
             parentOnly: ['CreditNoteID', 'UpdatedDateUTC']],
            /*[objectName: 'CreditNoteAttachment', listName: 'CreditNotes', pk: ['CreditNoteID', 'Attachments.AttachmentID'],
                childClass: 'Attachment', parentName: 'CreditNote', parentColumn: 'Attachments',
                parentOnly: ['CreditNoteID', 'UpdatedDateUTC']],*/

            [objectName: 'Currency', listName: 'Currencies', pk: ['Code']],

            [objectName: 'Employee', listName: 'Employees', pk: ['EmployeeID']],
            /*[objectName: 'EmployeeExternalLink', listName: 'Employees', pk: ['EmployeeID', 'ExternalLink.LinkID'],
                childClass: 'Hyperlink', parentName: 'Employee', parentColumn: 'ExternalLink',
                parentOnly: ['EmployeeID', 'UpdatedDateUTC']],*/

            [objectName: 'ExpenseClaim', listName: 'ExpenseClaims', pk: ['ExpenseClaimID']],
            [objectName: 'ExpenseClaimReceipt', listName: 'ExpenseClaims', pk: ['ExpenseClaimID', 'Receipts.ReceiptID'],
             childClass: 'Receipt', parentName: 'ExpenseClaim', parentColumn: 'Receipts',
             parentOnly: ['ExpenseClaimID', 'UpdatedDateUTC'], childOnly: ['ReceiptID']],
            [objectName: 'ExpenseClaimPayment', listName: 'ExpenseClaims', pk: ['ExpenseClaimID', 'Payments.PaymentID'],
             childClass: 'Payment', parentName: 'ExpenseClaim', parentColumn: 'Payments',
             parentOnly: ['ExpenseClaimID', 'UpdatedDateUTC'], childOnly: ['PaymentID']],

            [objectName: 'Invoice', listName: 'Invoices', pk: ['InvoiceID']],
            [objectName: 'InvoiceLineItem', listName: 'Invoices', pk: ['InvoiceID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'Invoice', parentColumn: 'LineItems',
             parentOnly: ['InvoiceID', 'UpdatedDateUTC']],
            /*[objectName: 'InvoicePayment', listName: 'Invoices', pk: ['InvoiceID', 'Payments.PaymentID'],
                childClass: 'Payment', parentName: 'Invoice', parentColumn: 'Payments',
                parentOnly: ['InvoiceID', 'UpdatedDateUTC'], childOnly: ['PaymentID']],
            [objectName: 'InvoicePrepayment', listName: 'Invoices', pk: ['InvoiceID', 'Prepayments.PrepaymentID'],
                childClass: 'Prepayment', parentName: 'Invoice', parentColumn: 'Prepayments',
                parentOnly: ['InvoiceID', 'UpdatedDateUTC'], childOnly: ['PrepaymentID']],
            [objectName: 'InvoiceOverpayment', listName: 'Invoices', pk: ['InvoiceID', 'Overpayments.OverpaymentID'],
                childClass: 'Overpayment', parentName: 'Invoice', parentColumn: 'Overpayments',
                parentOnly: ['InvoiceID', 'UpdatedDateUTC'], childOnly: ['OverpaymentID']],
            [objectName: 'InvoiceCreditNote', listName: 'Invoices', pk: ['InvoiceID', 'CreditNotes.CreditNoteID'],
                childClass: 'CreditNote', parentName: 'Invoice', parentColumn: 'CreditNotes',
                parentOnly: ['InvoiceID', 'UpdatedDateUTC'], childOnly: ['CreditNoteID']],
            [objectName: 'InvoiceAttachment', listName: 'Invoices', pk: ['InvoiceID', 'Attachments.AttachmentID'],
                childClass: 'Attachment', parentName: 'Invoice', parentColumn: 'Attachments',
                parentOnly: ['InvoiceID', 'UpdatedDateUTC'], childOnly: ['AttachmentID']],*/

            [objectName: 'Item', listName: 'Items', pk: ['ItemID'], xsdFile: 'Items.xsd'],

            [objectName: 'Journal', listName: 'Journals', pk: ['JournalID']],
            [objectName: 'JournalLine', listName: 'Journals', pk: ['JournalID', 'JournalLines.JournalLineID'],
             childClass: 'JournalLine', parentName: 'Journal', parentColumn: 'JournalLines',
             parentOnly: ['JournalID', 'CreatedDateUTC']],

            [objectName: 'ManualJournal', listName: 'ManualJournals', pk: ['ManualJournalID']],
            [objectName: 'ManualJournalLine', listName: 'ManualJournals', pk: ['ManualJournalID', 'JournalLines.AccountCode', 'JournalLines.TaxType'],
             childClass: 'ManualJournalLine', parentName: 'ManualJournal', parentColumn: 'JournalLines',
             parentOnly: ['ManualJournalID', 'UpdatedDateUTC']],

            [objectName: 'Organisation', listName: 'Organisations', clientParams: ' ', pk: ['OrganisationID']],
            [objectName: 'OrganisationAddress', listName: 'Organisations', clientParams: ' ', pk: ['OrganisationID', 'Addresses.AddressType'],
             childClass: 'Address', parentName: 'Organisation', parentColumn: 'Addresses',
             parentOnly: ['OrganisationID', 'CreatedDateUTC']],
            [objectName: 'OrganisationPhone', listName: 'Organisations', clientParams: ' ', pk: ['OrganisationID', 'Phones.PhoneType'],
             childClass: 'Phone', parentName: 'Organisation', parentColumn: 'Phones',
             parentOnly: ['OrganisationID', 'CreatedDateUTC']],
            /*[objectName: 'OrganisationExternalLink', listName: 'Organisations', pk: ['OrganisationID', 'ExternalLinks.LinkType'],
                childClass: 'externalLinks', parentName: 'Organisation', parentColumn: 'ExternalLinks',
                parentOnly: ['OrganisationID', 'UpdatedDateUTC']],*/
            /*[objectName: 'OrganisationPaymentTerm', listName: 'Organisations', pk: ['OrganisationID', 'PaymentTerms.Type'],
                childClass: 'paymentTerms', parentName: 'Organisation', parentColumn: 'PaymentTerms',
                parentOnly: ['OrganisationID', 'UpdatedDateUTC']],*/

            [objectName: 'Overpayment', listName: 'Overpayments', pk: ['OverpaymentID']],
            [objectName: 'OverpaymentLineItem', listName: 'Overpayments', pk: ['OverpaymentID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'Overpayment', parentColumn: 'LineItems',
             parentOnly: ['OverpaymentID', 'UpdatedDateUTC']],
            [objectName: 'OverpaymentAllocation', listName: 'Overpayments', pk: ['OverpaymentID', 'Allocations.Invoice.InvoiceID', 'Allocations.Date'],
             childClass: 'Allocation', parentName: 'Overpayment', parentColumn: 'Allocations',
             parentOnly: ['OverpaymentID', 'UpdatedDateUTC']],
            /*[objectName: 'OverpaymentPayment', listName: 'Overpayments', pk: ['OverpaymentID', 'Payments.PaymentID'],
                childClass: 'Payment', parentName: 'Overpayment', parentColumn: 'Payments',
                parentOnly: ['OverpaymentID', 'UpdatedDateUTC'], childOnly: ['PaymentID']],*/

            [objectName: 'Payment', listName: 'Payments', pk: ['PaymentID']],

            [objectName: 'Prepayment', listName: 'Prepayments', pk: ['PrepaymentID']],
            [objectName: 'PrepaymentLineItem', listName: 'Prepayments', pk: ['PrepaymentID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'Prepayment', parentColumn: 'LineItems',
             parentOnly: ['PrepaymentID', 'UpdatedDateUTC']],
            [objectName: 'PrepaymentAllocation', listName: 'Prepayments', pk: ['PrepaymentID', 'Allocations.Invoice.InvoiceID', 'Allocations.Date'],
             childClass: 'Allocation', parentName: 'Prepayment', parentColumn: 'Allocations',
             parentOnly: ['PrepaymentID', 'UpdatedDateUTC']],

            [objectName: 'PurchaseOrder', listName: 'PurchaseOrders', pk: ['PurchaseOrderID']],
            [objectName: 'PurchaseOrderLineItem', listName: 'PurchaseOrders', pk: ['PurchaseOrderID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'PurchaseOrder', parentColumn: 'LineItems',
             parentOnly: ['PurchaseOrderID', 'UpdatedDateUTC']],

            [objectName: 'Receipt', listName: 'Receipts', pk: ['ReceiptID']],
            [objectName: 'ReceiptLineItem', listName: 'Receipts', pk: ['ReceiptID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'Receipt', parentColumn: 'LineItems',
             parentOnly: ['ReceiptID', 'UpdatedDateUTC']],
            /*[objectName: 'ReceiptAttachment', listName: 'Receipts', pk: ['ReceiptID', 'Attachments.AttachmentID'],
                childClass: 'Attachment', parentName: 'Receipt', parentColumn: 'Attachments',
                parentOnly: ['ReceiptID', 'UpdatedDateUTC']],*/

            [objectName: 'RepeatingInvoice', listName: 'RepeatingInvoices', pk: ['RepeatingInvoiceID']],
            [objectName: 'RepeatingInvoiceLineItem', listName: 'RepeatingInvoices', pk: ['RepeatingInvoiceID', 'LineItems.LineItemID'],
             childClass: 'LineItem', parentName: 'RepeatingInvoice', parentColumn: 'LineItems',
             parentOnly: ['RepeatingInvoiceID', 'Schedule.StartDate']],
            /*[objectName: 'Report', listName: 'Reports'],*/

            [objectName: 'TaxRate', listName: 'TaxRates', pk: ['Name']],
            [objectName: 'TaxRateComponent', listName: 'TaxRates', pk: ['Name', 'TaxComponents.Name'],
             childClass: 'TaxComponent', parentName: 'TaxRate', parentColumn: 'TaxComponents',
             parentOnly: ['Name', 'UpdatedDateUTC']],

            [objectName: 'TrackingCategory', listName: 'TrackingCategories', pk: ['TrackingCategoryID'],
             xsdFile: 'Tracking.xsd', clientParams: 'modifiedAfter, where, order, includeArchived'],
            [objectName: 'TrackingCategoryOption', listName: 'TrackingCategories', pk: ['TrackingCategoryID', 'Options.TrackingOptionID'],
             xsdFile: 'Tracking.xsd',
             clientParams: 'modifiedAfter, where, order, includeArchived',
             childClass: 'TrackingCategoryOption', childMethod: 'Option',
             parentName: 'TrackingCategory', parentColumn: 'Options',
             parentOnly: ['TrackingCategoryID', 'UpdatedDateUTC']],

            [objectName: 'User', listName: 'Users', pk: ['UserID']]
    ]

    def static objectScripts = [
            ContactBalance: '''
        if (limit > 0 && res > limit) return
        def row = [:] as Map<String, Object>

        def countChild = 0
        master.getBalances()?.getAccountsPayable()?.each { com.xero.model.AccountsPayable child ->
            countChild++
            row.put('balances.accountspayable.outstanding', child.getOutstanding())
            row.put('balances.accountspayable.overdue', child.getOverdue())
        }
        master.getBalances()?.getAccountsReceivable()?.each { com.xero.model.AccountsReceivable child ->
            countChild++
            row.put('balances.accountsreceivable.outstanding', child.getOutstanding())
            row.put('balances.accountsreceivable.overdue', child.getOverdue())
        }

        assert countChild <= 2, "Invalid \\"balances\\" field for \\"" + master.getContactID() + "\\" Contact Xero Object!"
        if (countChild > 0) {
            row.put('contactid', master.getContactID())
            row.put('updateddateutc', (master.getUpdatedDateUTC() as Calendar)?.time)
            if (filter == null || filter(row)) {
                code(row)
                res++
            }
        }
'''
    ]

    @Override
    List<Object> retrieveObjects(Map params, Closure filter) {
        saveToHistory("Retrieving list of objects with params $params")
        def res = [] as List<Map>
        objects.each { Map obj ->
            def added = true
            if (params.xeroListName != null && obj.listName != params.xeroListName) added = false
            if (added && params.xeroObjectName != null && obj.objectName != params.xeroObjectName) added = false
            if (added && filter != null && !filter.call(obj)) added = false
            if (added) res << obj
        }

        return res
    }

    @Override
    List<Field> fields(Dataset dataset) {
        if (dataset.objectName == null)
            throw new ExceptionGETL('Required "objectName" property for dataset')

        def ds = dataset as XeroDataset

        def objList = retrieveObjects([xeroObjectName: ds.xeroObjectName], null)
        if (objList.isEmpty())
            throw new ExceptionGETL("Xero object \"${ds.xeroObjectName}\" not found!")

        def objParams = objList[0] as Map

        def mainClass = objParams.parentName?:objParams.objectName
        def xsdFile = objParams.xsdFile?:"${mainClass}.xsd"

        def xsdMap = MapUtils.XsdFromResource(xsdPath, xsdFile)
        def fields = MapUtils.XsdMap2Fields(xsdMap, mainClass)

        def parentOnly = objParams.parentOnly as List<String>
        def childOnly = objParams.childOnly as List<String>
        def parentColumn = objParams.parentColumn
        def parentPK = objParams.pk as List<String>

        def res = [] as List<Field>
        fields.each { Field f ->
            if (parentOnly != null && !(f.name in parentOnly)) return
            if (parentColumn != null) f.extended.isParent = true

            def useType = f.extended.useType
            if (useType != null) {
                def typeParams = objects.find { it.objectName == useType }
                if (typeParams?.pk != null) {
                    def childNames = f.name.split('[.]')
                    if (!(childNames[childNames.length - 1] in typeParams.pk)) return
                }
            }

            if (f.type != Field.Type.OBJECT) res << f
        }

        if (parentColumn != null) {
            def parentField = fields.find { Field f -> f.name == parentColumn }
            if (parentField == null)
                throw new ExceptionGETL("Parent column \"${parentColumn}\" from object \"$mainClass\" not found!")
            if (parentField.type != Field.Type.OBJECT || parentField.extended.sequenceType == null)
                throw new ExceptionGETL("Invalid parent column \"${parentColumn}\" from object \"$mainClass\"!")

            def childClass = objParams.childClass
            def objectFields = MapUtils.XsdMap2Fields(xsdMap, childClass)
            objectFields.each { Field f ->
                if (childOnly != null && !(f.name in childOnly)) return

                def useType = f.extended.useType
                if (useType != null) {
                    def typeParams = objects.find { it.objectName == useType }
                    if (typeParams?.pk != null) {
                        def childNames = f.name.split('[.]')
                        if (!(childNames[childNames.length - 1] in typeParams.pk)) return
                    }
                }

                f.name = parentColumn + '.' + f.name
                if (f.type != Field.Type.OBJECT) res << f
            }
        }

        def pkOrd = 0
        parentPK?.each { String fieldName ->
            def field = res.find { it.name == fieldName }
            if (field == null) {
                def avaibleFields = res*.name
                throw new ExceptionGETL("Primary key \"$fieldName\" for \"$ds.xeroObjectName\" not found, current fields: $avaibleFields!")
            }

            pkOrd++
            field.isKey = true
            field.ordKey = pkOrd
        }

        res.each { Field f ->
            f.extended.originalName = f.name
            f.name = f.name.replace('.', '_')
        }

        return res
    }

    @Override
    void startTran() { throw new ExceptionGETL('Not support this features!') }

    @Override
    void commitTran() { throw new ExceptionGETL('Not support this features!') }

    @Override
    void rollbackTran() { throw new ExceptionGETL('Not support this features!') }

    @Override
    void createDataset(Dataset dataset, Map params) { throw new ExceptionGETL('Not support this features!') }

    @Override
    void openWrite(Dataset dataset, Map params, Closure prepareCode) { throw new ExceptionGETL('Not support this features!') }

    @Override
    void write(Dataset dataset, Map row) { throw new ExceptionGETL('Not support this features!') }

    @Override
    void doneWrite(Dataset dataset) { throw new ExceptionGETL('Not support this features!') }

    @Override
    void closeWrite(Dataset dataset) { throw new ExceptionGETL('Not support this features!') }

    @Override
    void bulkLoadFile(CSVDataset source, Dataset dest, Map params, Closure prepareCode) { throw new ExceptionGETL('Not support this features!') }

    @Override
    void clearDataset(Dataset dataset, Map params) { throw new ExceptionGETL('Not support this features!') }

    @Override
    long executeCommand(String command, Map params) { throw new ExceptionGETL('Not support this features!') }

    @Override
    long getSequence(String sequenceName) { throw new ExceptionGETL('Not support this features!') }

    private static String ConvertFieldName(Field.Type type, String name, boolean isChild = false) {
        def list = name.split('[.]')
        def start = (isChild)?1:0
        def res = [] as List<String>
        for (int i = start; i < list.length; i++) {
            if (type != Field.Type.BOOLEAN || i < list.length - 1)
                res << 'get' + ((list[i] != 'Class')?list[i]:'Clazz') + '()'
            else
                res << 'is' + ((list[i] != 'Class')?list[i]:'Clazz') + '()'
        }
        return res.join('?.')
    }

    @Override
    long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        def ds = dataset as XeroDataset
        def objectName = ds.xeroObjectName
        saveToHistory("read $objectName with params: $params")

        def dsParams = objects.find { it.objectName == objectName }
        if (dsParams == null) throw new ExceptionGETL("Xero object \"$objectName\" not found!")

        Date modifiedAfter = (params.modifiedAfter instanceof Date)?params.modifiedAfter:DateUtils.ParseDate('yyyy-MM-dd\'T\'HH:mm:ss.SSS', params.modifiedAfter)
        def where = params.where as String
        def order = params.orderBY as String
        def limit = Long.valueOf(params.limit?:0)
        Closure filter = (Closure)params.filter

        def fieldAttrs = fields(ds)
        if (ds.field.isEmpty()) ds.field = fieldAttrs
        List<String> fields = ds.field*.name
        if (prepareCode != null) fields = (ArrayList<String>)prepareCode.call(ds.field)

        def listName = dsParams.listName as String
        def parentName = dsParams.parentName as String
        def parentColumn = dsParams.parentColumn as String
//        def parentOnly = dsParams.parentOnly as List<String>
//        def childOnly = dsParams.childOnly as List<String>
        def childClass = dsParams.childClass
        def childMethod = dsParams.childMethod?:childClass
        def clientParams = dsParams.clientParams?:'modifiedAfter, where, order'
        def includeArchived = BoolUtils.IsValue(dsParams.includeArchived)

        def mainClass = parentName?:objectName
//        if (parentOnly != null) parentOnly = parentOnly*.toLowerCase()
//        if (childOnly != null) childOnly = childOnly*.toLowerCase()

        def pb = new StringBuilder()
        def cb = new StringBuilder()

        if (objectScripts."$objectName" != null) {
            pb.append(objectScripts."$objectName")
        }
        else {
            if (parentColumn == null) {
                pb << '\t\tif (limit > 0 && res > limit) return\n'
                pb << '\t\tdef row = [:] as Map<String, Object>\n'
                cb << ''
            } else {
                pb << '\t\tif (limit > 0 && res > limit) return\n'
                pb << '\t\tdef mainRow = [:] as Map<String, Object>\n'
                cb << "\t\tmaster.${ConvertFieldName(Field.Type.OBJECT, parentColumn)}?.${ConvertFieldName(Field.Type.OBJECT, childMethod)}?.each { com.xero.model.${childClass} child ->\n"
                cb << '\t\t\tif (limit > 0 && res > limit) return\n'
                cb << '\t\t\tdef row = [:] as Map<String, Object>\n'
            }

            ds.field.clear()
            fields.each { String fieldName ->
                def fn = fieldName.toLowerCase()
                def field = fieldAttrs.find { Field f -> f.name.toLowerCase() == fn }
                if (field == null) throw new ExceptionGETL("Field \"$fieldName\" from dataset \"$ds\" not found!")
                ds.field << field

                if (parentColumn == null) {
                    def pn = ConvertFieldName(field.type, field.extended.originalName)
                    if (!(field.type in [Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME]))
                        pb << "\t\trow.put('$fn', master.$pn)\n"
                    else
                        pb << "\t\trow.put('$fn', (master.$pn as Calendar)?.time)\n"
                } else {
                    if (field.extended.isParent) {
                        def pn = ConvertFieldName(field.type, field.extended.originalName)
                        if (!(field.type in [Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME]))
                            pb << "\t\tmainRow.put('$fn', master.$pn)\n"
                        else
                            pb << "\t\tmainRow.put('$fn', (master.$pn as Calendar)?.time)\n"
                    } else {
                        def pn = ConvertFieldName(field.type, field.extended.originalName, true)
                        if (!(field.type in [Field.Type.DATETIME, Field.Type.DATE, Field.Type.TIME]))
                            cb << "\t\t\trow.put('$fn', child.$pn)\n"
                        else
                            cb << "\t\t\trow.put('$fn', (child.$pn as Calendar)?.time)\n"
                    }
                }
            }
            if (parentColumn != null) {
                cb << '''            row.putAll(mainRow)
            if (filter == null || filter(row)) {
                code(row)
                res++
            }
        }
'''
            } else {
                pb << '''        if (filter == null || filter(row)) {
            code(row)
            res++
        }
'''
            }
        }

        /*println this.getClass().getResourceAsStream('/config/demo.conf').text
        println this.getClass().getResourceAsStream('/certs/public_privatekey.pfx').text*/

        def sb = new StringBuilder()
        sb << """
{ com.xero.api.XeroClient client, Date modifiedAfter, String where, String order, Long limit, boolean includeArchived, Closure filter, Closure code ->
    def rows = client.get$listName($clientParams)
    Long res = 0
    rows.each { com.xero.model.${mainClass} master ->
${pb.toString()}
${cb.toString()}
    }

    return res
}"""
        def closure = GenerationUtils.EvalGroovyClosure(sb.toString())

        Long res
        try {
            res = closure.call(client, modifiedAfter, where, order, limit, includeArchived, filter, code) as Long
        }
        catch (Exception e) {
            Logs.Severe("Error read Xero object \"$ds.objectName\"")
            Logs.Dump(e, 'Xero', ds.objectName, sb.toString())
            throw e
        }

        return res
    }
}