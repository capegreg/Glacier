/***
	Glacier \ Amazon S3 Glacier
	Created by: GBologna
	Created On: 3/18/2020
	Updated: 02/12/2021, gwb, adjust inventory request dates
	Git: repos\Glacier

	-> Handles AWS vault and archive inventory jobs

	-> Flags document archives that were pureged in AWS S3 Glacier
	-> Tables IDOCS.idocs_documents and IDOCS.idocx_field 
		are considered the archive table and the document 
		tables respectively.

	DOCUMENT TERMS
	1. ARCHVD: Flag indicating that a document was archived
	2. PENDING PURGE: Flag indicating that a document can be deleted from AWS and disk
	3. PURGED: Flag indicating that a document was deleted from AWS and is awaiting audit 
		for purging in the inventory process
	4. DELETD: Flag indicating that a document was audited in the inventory process and is 
		shall be purged in the delete process (Special system delete flags include: DELETD_NS2 & DELETD_OS2)
	5. PENDING RESTORE: Flag indicating that a request has been made for an archive restore from AWS
	6. RESTORE REQUESTED: Flag indicating that an archive retrieval job was initiated per a 
		PENDING RESTORE request

	TODO: find a better way to return results and not use fetch max rows
	TODO: refactor inventory requests to its own module
 */

const isDebugMode = process.env.DEBUG_MODE;

// the AWS vault where all archives are kept
const removed = 'removed';
// rest api
const PaoGlacier = require('./PaoGlacier');
// PaoNas nas parent location
const PaoNas = require('./PaoNas');
// PaoGlacier object
let glacier = undefined;
const PaoRestorer = require('./PaoRestorer');
// error logging
const PaoLogger = require('./PaoLogger');
// file handling
const path = require('path');
const fs = require('fs');
// API for decoding Buffer objects into strings
const { StringDecoder } = require('string_decoder');
const decoder = new StringDecoder('utf8');
// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

// nas is global
const nas = new PaoNas();

/* *********************************************************************************************
	setPurgedDateRangeObject
 	Use to include in inventory request for purge date range

 	Example min\max dates are derived by getting the min and max date of all purged documents:
 		FILE_TYPE,CREATE_DATE
		PURGED,15-JUN-20
	When there is only a single purged document, both min and max will be the same
	Min = 2020-06-15T16:00:00Z
	Max	=	2020-06-15T16:00:00Z
********************************************************************************************* */
const setPurgedDateRangeObject = () => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.autoCommit = false;
	oracledb.extendedMetaData = false;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		var bindvars = {	
			v_iso8601_min:  { type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_iso8601_max: 	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_utc_min:  	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_utc_max: 	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_nls_min:  	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_nls_max: 	{ type: oracledb.STRING, dir: oracledb.BIND_OUT }						
		}
		const result = connection.execute(
			`BEGIN pkg_purge_services.prc_get_purged_date_range(
					  :v_iso8601_min
					, :v_iso8601_max
					, :v_utc_min
					, :v_utc_max
					, :v_nls_min
					, :v_nls_max); END;`,
			bindvars,
		)
		.then(result => {
			connection.close();

			var hasDate = false;
			var invDate;
			
			if(result.outBinds.v_iso8601_min !== null) {

				// ex. 2020-06-15T16:00:00Z

				invDate = new Date(result.outBinds.v_iso8601_min);
				purgedDateRange.ISO8601.StartDate = invDate.toISOString().split('.')[0]+"Z"; // remove ms
				// purgedDateRange.ISO8601.StartDate = result.outBinds.v_iso8601_min;

				hasDate = true;

			}
			if(result.outBinds.v_iso8601_max !== null) {

				// ex. 2020-06-15T16:00:00Z

				invDate = new Date(result.outBinds.v_iso8601_max);
				purgedDateRange.ISO8601.EndDate = invDate.toISOString().split('.')[0]+"Z"; // remove ms
				//purgedDateRange.ISO8601.EndDate = result.outBinds.v_iso8601_max;
				hasDate = true;

			}				
			if(result.outBinds.v_utc_min !== null) {
				
				// ex. 06-15-2020 00:00:00

				// invDate = new Date(result.outBinds.v_utc_min);
				// purgedDateRange.UTC.StartDate = invDate.toISOString().split('.')[0]; // remove ms
				purgedDateRange.UTC.StartDate = result.outBinds.v_utc_min;
				hasDate = true;

			}				
			if(result.outBinds.v_utc_max !== null) {

				// ex. 06-15-2020 00:00:00

				// invDate = new Date(result.outBinds.v_utc_max);
				// purgedDateRange.UTC.EndDate = invDate.toISOString().split('.')[0]; // remove ms
				purgedDateRange.UTC.EndDate = result.outBinds.v_utc_max;
				hasDate = true;

			}
			if(result.outBinds.v_nls_min !== null) {

				// ex. 15-JUN-20 00:00:00

				// invDate = new Date(result.outBinds.v_nls_min);
				// purgedDateRange.NLS.StartDate = invDate.toISOString().split('.')[0]; // remove ms				
				purgedDateRange.NLS.StartDate = result.outBinds.v_nls_min;
				hasDate = true;

			}
			if(result.outBinds.v_nls_max !== null) {

				// ex. 15-JUN-20 00:00:00

				// invDate = new Date(result.outBinds.v_nls_max);
				// purgedDateRange.NLS.EndDate = invDate.toISOString().split('.')[0]; // remove ms	
				purgedDateRange.NLS.EndDate = result.outBinds.v_nls_max;				
				hasDate = true;

			}

			if(hasDate) {
				// restrict inventory date range
				if(restrictInventoryRequestDateRange()) {
					resolve(true);
				} else {
					reject(false);
				}
			} else {
				resolve(true);
			}
			
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`setPurgedDateRangeObject().result -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`setPurgedDateRangeObject() -> ${error}`);
		reject(error);
	});
});

/* *********************************************************************************************
Reduce the inventory output size by shortening the EndDate.
because 1 day of inventory may be thousands of archives
Fine tune as needed
********************************************************************************************* */
function restrictInventoryRequestDateRange() {
	try {

		for (var i in purgedDateRange) {
			// hasOwnProperty() will filter properties from the object's prototype chain
			if (purgedDateRange.hasOwnProperty(i)) {

				let startDate = new Date(purgedDateRange[i].StartDate);
				let endDate = new Date(purgedDateRange[i].EndDate);
		
				var Difference_In_Time = endDate.getTime() - startDate.getTime(); 
				var Difference_In_Days = Math.round(Difference_In_Time / (1000 * 3600 * 24)); 
				
				// Add 2 days rule
				// Do not request inventories gt 2 days				
				// if end date > 2 days, take start date and add 2 days
				if(Difference_In_Days > 2) {
					endDate = new Date(startDate);
					endDate.setDate(endDate.getDate() + 2);

					if(i==='ISO8601') {
						endDate = endDate.toISOString().split('.')[0]+"Z"; // remove ms
					}

					purgedDateRange[i].EndDate = endDate;
				}	
				// Add 1 day rule
				// Do not request inventories gt 1 day
				// AWS error if dates are eq, range must be gt than zero
				// if start and end date same, take start date and add 1 day
				if(Difference_In_Days === 0) {
					endDate = new Date(startDate);
					endDate.setDate(endDate.getDate() + 1);

					if(i==='ISO8601') {
						endDate = endDate.toISOString().split('.')[0]+"Z"; // remove ms
					}

					purgedDateRange[i].EndDate = endDate;
				}					
							
			}
		}
		return true;

	} catch (error) {
		loggerError.logError(`restrictInventoryRequestDateRange() -> ${error}`);
		return false;
	}
}

/* *********************************************************************************************
 setRestoreDateRangeObject
 Use to include in archive request for restores
********************************************************************************************* */
const setRestoreDateRangeObject = () => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.autoCommit = false;
	oracledb.extendedMetaData = false;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		var bindvars = {	
			v_iso8601_min:  { type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_iso8601_max: 	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_utc_min:  	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_utc_max: 	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_nls_min:  	{ type: oracledb.STRING, dir: oracledb.BIND_OUT },
			v_nls_max: 	{ type: oracledb.STRING, dir: oracledb.BIND_OUT }						
		}
		const result = connection.execute(
			`BEGIN pkg_purge_services.prc_get_restore_date_range(
					  :v_iso8601_min
					, :v_iso8601_max
					, :v_utc_min
					, :v_utc_max
					, :v_nls_min
					, :v_nls_max); END;`,
			bindvars,
		)
		.then(result => {
			connection.close();

			if(result.outBinds.v_iso8601_min !== null)
				restoreDateRange.ISO8601.StartDate = result.outBinds.v_iso8601_min;

			if(result.outBinds.v_iso8601_max !== null)
				restoreDateRange.ISO8601.EndDate = result.outBinds.v_iso8601_max;

			if(result.outBinds.v_utc_min !== null)
				restoreDateRange.UTC.StartDate = result.outBinds.v_utc_min;

			if(result.outBinds.v_utc_max !== null)
				restoreDateRange.UTC.EndDate = result.outBinds.v_utc_max;

			if(result.outBinds.v_nls_min !== null)
				restoreDateRange.NLS.StartDate = result.outBinds.v_nls_min;

			if(result.outBinds.v_nls_max !== null)
				restoreDateRange.NLS.EndDate = result.outBinds.v_nls_max;				

			resolve(true);
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`setRestoreDateRangeObject().result -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`setRestoreDateRangeObject() -> ${error}`);
		reject(error);
	});
});

/* *********************************************************************************************
 buildPurgedDocumentsMap
 Result is all documents marked PURGED
 Use to compare documents that do not exist in AWS inventory
********************************************************************************************* */
const buildPurgedDocumentsMap = (inventoryDate, inventoryCreationDate) => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.extendedMetaData = true;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		
		var bindvars = {	
			v_inventory_date: inventoryDate,
			v_creation_date: inventoryCreationDate,
			cursor:  { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
		}
		// The PL/SQL has an OUT bind of type SYS_REFCURSOR
		const result = connection.execute(
		  "BEGIN pkg_purge_services.prc_get_docs_purged_aws(:v_inventory_date, :v_creation_date, :cursor); END;",
			bindvars,
    )
		.then(result => {			
			fetchInventoryRowsFromRS(connection, result.outBinds.cursor, 1000)
			.then(result => {
				connection.close();
				resolve(true);
			});
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`buildPurgedDocumentsMap().fetchInventoryRowsFromRS -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`buildPurgedDocumentsMap() -> ${error}`);
	});
});

/* *********************************************************************************************
	fetchRowsFromRS
	populates purgedDocumentsMap data object with documents to process
********************************************************************************************* */
const fetchInventoryRowsFromRS = (connection, resultSet, numRows) => new Promise((resolve, reject) => {
	resultSet.getRows(// get numRows rows
		numRows,			
		function (error, rows) {
		if (error) {
			resultSet.close();
			loggerError.logError(`fetchInventoryRowsFromRS() -> ${error}`);
			reject(error);
		} else if (rows.length == 0) {  // no rows, or no more rows
			resultSet.close();
			resolve(0);
		} else if (rows.length > 0) {
			rows.forEach(function(row) {

				purgedDocumentsMap.set(
					row.DOCUMENTID, {
						"documentId": row.DOCUMENTID,
						"createdOn": row.CREATEDON,
						"title_meta": row.TITLE_META,
						"metadata": row.METADATA
					});

			});
			resultSet.close();
			resolve(rows.length);
		}
	});
});

/* *********************************************************************************************
 getRestoreDocuments
********************************************************************************************* */
const getRestoreDocuments = () => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.extendedMetaData = true;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		// The PL/SQL has an OUT bind of type SYS_REFCURSOR
		var bindvars = {	
			cursor:  { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
		}
		const result = connection.execute(
		  "BEGIN pkg_restore_services.prc_get_docs_to_restore(:cursor); END;",
			bindvars,
    )
		.then(result => {
			fetchRestoreRowsFromRS(connection, result.outBinds.cursor, 1000)
			.then(result => {
				connection.close();
				resolve(true);
			});
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`getRestoreDocuments().fetchRestoreRowsFromRS -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`getRestoreDocuments() -> ${error}`);
		reject(false);
	});
});

/* *********************************************************************************************
	fetchRowsFromRS
	populates restoreMap data object with documents to process
********************************************************************************************* */
const fetchRestoreRowsFromRS = (connection, resultSet, numRows) => new Promise((resolve, reject) => {
	resultSet.getRows(// get numRows rows
		numRows,			
		function (error, rows) {
		if (error) {
			resultSet.close();		
			loggerError.logError(`fetchRestoreRowsFromRS() -> ${error}`);
			reject(error);
		} else if (rows.length == 0) {  // no rows, or no more rows
			resultSet.close();
			resolve(0);
		} else if (rows.length > 0) {
			rows.forEach(function(row) {

				restoreMap.set(row.DOCUMENTID, {
					"procedure": row.PROCEDURE,
					"filename": row.FILENAME,
					"fileSystemId": row.FILESYSTEMID, 
					"fileType": row.FILETYPE,
					"fileSizeInBytes": row.FILESIZEBYTES,
					"documentType": row.DOCUMENTTYPE,
					"metadata": row.METADATA
				});

			});
			resultSet.close();
			resolve(rows.length);
		}
	});
});

/* *********************************************************************************************
 getRestoreJobs
********************************************************************************************* */
const getRestoreJobs = () => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.extendedMetaData = true;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		// The PL/SQL has an OUT bind of type SYS_REFCURSOR
		var bindvars = {	
			cursor:  { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
		}
		const result = connection.execute(
		  "BEGIN pkg_restore_services.prc_get_restore_jobs(:cursor); END;",
			bindvars,
    )
		.then(result => {
			fetchRestoreJobsRowsFromRS(connection, result.outBinds.cursor, 200)
			.then(result => {
				connection.close();
				resolve(true);
			});
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`getRestoreJobs().fetchRestoreJobsRowsFromRS -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`getRestoreJobs() -> ${error}`);
		reject(false);
	});
});

/* *********************************************************************************************
	fetchRestoreJobsRowsFromRS
	populates restoreMap data object with documents to process
********************************************************************************************* */
const fetchRestoreJobsRowsFromRS = (connection, resultSet, numRows) => new Promise((resolve, reject) => {
	resultSet.getRows(// get numRows rows
		numRows,			
		function (error, rows) {
		if (error) {
			resultSet.close();		
			loggerError.logError(`fetchRestoreJobsRowsFromRS() -> ${error}`);
			reject(error);
		} else if (rows.length == 0) {  // no rows, or no more rows
			resultSet.close();
			resolve(0);
		} else if (rows.length > 0) {
			rows.forEach(function(row) {

				restoreJobsMap.set(row.DOCUMENTID, {
					"procedure": row.PROCEDURE,
					"filename": row.FILENAME,
					"fileSystemId": row.FILESYSTEMID, 
					"fileType": row.FILETYPE,
					"fileSizeInBytes": row.FILESIZEBYTES,
					"documentType": row.DOCUMENTTYPE,
					"metadata": row.METADATA
				});

			});
			resultSet.close();
			resolve(rows.length);
		}
	});
});

/* ******************************************************
	The document was audited in inventory process and 
	can be flagged for deletion for delete process.
	This is an executeMany statement. The bind will
	 create a statement for every binded Id.
	TODO: move to procedure
****************************************************** */
const setArchiveDeleted = (purgeArray) => new Promise((resolve, reject) => {

 	let connection;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(c => {
		connection = c;
		let sql = `UPDATE IDOCS.idocs_documents SET file_type = 'DELETD' WHERE document_id = :0`;
	
		let binds = [];
		let ids = purgeArray.map(function(id){
			binds.push([id]);
		}).join();
		
		// the binds variable array example
		// [3338118], [4976518]
		// The position of the array values corresponds to the position of the SQL
		// bind parameters as they occur in the statement, regardless of their names.
		let options = {
			autoCommit: true,
			batchErrors: true, // no commit occurs if data error
      			bindDefs: [
				{ type: oracledb.NUMBER }	// DOCUMENT_ID
      			]			
		};
		connection.executeMany(sql, binds, options)
		.then(result => {			
			connection.close();
			resolve(result.rowsAffected);
		})
		.catch(error => {
			connection.close();
			loggerError.logError(`setArchiveDeleted() -> ${error}`);
			reject(error);
		});
	});

});

/* ******************************************************
	The document was audited in inventory process and 
	can be flagged for deletion for delete process
	This is an executeMany statement. The bind will
	 create a statement for every binded Id.
****************************************************** */
const setDocumentDeleted = (purgeArray) => new Promise((resolve, reject) => {
 	let connection;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(c => {		
		connection = c;
	  let sql = `UPDATE idocs.idocx_field f 
		SET f.string_value = 'DELETD' || REPLACE(f.string_value, 'PURGED')
		WHERE f.name in ('Category', 'Photo Category') 
		AND f.string_value LIKE 'PURGED%' 
		AND document = :0`;
	
		let binds = [];
		let ids = purgeArray.map(function(id){
			binds.push([id]);
		}).join();
		
		// the binds variable array example
		// [3338118], [4976518]
		// The position of the array values corresponds to the position of the SQL
		// bind parameters as they occur in the statement, regardless of their names.
		let options = {
			autoCommit: true,
			batchErrors: true, // no commit occurs if data error
      			bindDefs: [
				{ type: oracledb.NUMBER }	// DOCUMENT_ID
      			]			
		};
		connection.executeMany(sql, binds, options)
		.then(result => {			
			connection.close();
			resolve(result.rowsAffected);
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`setDocumentDeleted() -> ${error}`);
			reject(error);
		});
	});
});

/* ******************************************************
	The document was audited in inventory process and 
	can be flagged for deletion for delete process
****************************************************** */
const setRestoreRequested = (docId) => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.autoCommit = false;
	oracledb.extendedMetaData = false;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		var bindvars = {	
			rowcount:  	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			return_code: 	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			id: docId
		}
		const result = connection.execute(
			"BEGIN pkg_restore_services.prc_upd_docs_restore_requested(:rowcount, :return_code, :id); END;",
			bindvars,
		)
		.then(result => {
			connection.close();
			var response = {
				rowCount: result.outBinds.rowcount,
				returnCode: result.outBinds.return_code
			};
			resolve(response);
		})
		.catch( error => {			
			connection.close();
			loggerError.logError(`setRestoreRequested().response -> ${error}`);
			reject(false);
		});		
	})
	.catch(function(error) {
		loggerError.logError(`setRestoreRequested() -> ${error}`);
		reject(error);
	});

});

/* ******************************************************
 setRestoreCompleted
	The document restore was completed
****************************************************** */
const setRestoreCompleted = (docId) => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.autoCommit = false;
	oracledb.extendedMetaData = false;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		var bindvars = {	
			rowcount:  	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			return_code: 	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			id: docId
		}
		const result = connection.execute(
			"BEGIN pkg_restore_services.prc_upd_docs_restore_done(:rowcount, :return_code, :id); END;",
			bindvars,
		)
		.then(result => {
			connection.close();
			var response = {
				rowCount: result.outBinds.rowcount,
				returnCode: result.outBinds.return_code
			};
			resolve(response);
		})
		.catch( error => {			
			connection.close();
			loggerError.logError(`setRestoreCompleted().response -> ${error}`);
			reject(false);
		});		
	})
	.catch(function(error) {
		loggerError.logError(`setRestoreCompleted() -> ${error}`);
		reject(error);
	});

});

/* ******************************************************
****************************************************** */
const updatePurgedArchivesJobId = (jobId) => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.autoCommit = false;
	oracledb.extendedMetaData = false;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		var bindvars = {	
			job_id:		jobId,
			start_date: 	purgedDateRange.NLS.StartDate,
			end_date: 	purgedDateRange.NLS.EndDate,			
			rowcount:  	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			return_code: 	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT }
		}
		const result = connection.execute(
			"BEGIN pkg_purge_services.prc_upd_purged_jobid(:job_id, :start_date, :end_date, :rowcount, :return_code); END;",
			bindvars,
		)
		.then(result => {
			connection.close();
			var response = {
				rowCount: result.outBinds.rowcount,
				returnCode: result.outBinds.return_code
			};
			resolve(response);
		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`updatePurgedArchivesJobId().response -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`updatePurgedArchivesJobId() -> ${error}`);
		reject(error);
	});

});

/* *********************************************************************************************
	Inventory audit.  
	Requests an inventory report from AWS based on the min and max dates of purged documents.
	AWS processes inventories 

	It typically takes 4 to 6 hours and up before an inventory is available for retrieval.

	https://docs.aws.amazon.com/amazonglacier/latest/dev/vault-inventory.html

	You can see the created inventory using CLI in powershell

	Example:

	PS C:\WINDOWS\system32> aws glacier list-jobs --account-id - --vault-name removed
	Result:

	- Action: InventoryRetrieval
  Completed: false
  CreationDate: '2021-01-25T18:58:28.828Z'
  InventoryRetrievalParameters:
    EndDate: '2021-01-22T08:10:49Z'
    Format: JSON
    StartDate: '2021-01-22T07:10:49Z'
  JobDescription: Inventory requested on Mon Jan 25 2021 13:58:26 GMT-0500 (Eastern
    Standard Time)
  JobId: xxxxxxxxxxxx
  StatusCode: InProgress
	VaultARN: xxxxxxxxxxxx

	A job ID will not expire for at least 24 hours after Glacier completes the job.

 	AWS date format: ISO-8601
 	The date range in Universal Coordinated Time (UTC) for vault inventory retrieval that 
 	includes archives created on or after this date. 
	 This value should be a string in the ISO 8601 date format, 
	 
	 Example:
		 good 2013-03-20T17:03:43Z
		 Do not pass ms
		 bad 2013-03-20T17:03:43.000Z

********************************************************************************************* */
const requestNewInventoryJob = async () => {
	try {
		// do not request a new inventory if one was already requested today
		if(!(todaysInventoryRequests.length > 0)) {
		
			// must have both an EndDate and a StartDate
			// for inventory management
			if(purgedDateRange.ISO8601.StartDate !== 0 && purgedDateRange.ISO8601.EndDate !== 0) {

				var InventoryRetrievalParameters = {}
				
				// AWS error, range must be greater than zero
				// AWS date cannot include ms
				// AWS will error if StartDate = EndDate, AWS inventory will include all archives after
				// StartDate when EndDate is missing. We do not want that, so if dates equal,
				// add an hour to the EndDate in order to include it without error from AWS
				// 1 hour may not be enough to return a complete inventory. add one day to EndDate
				// see prc_get_purged_date_range

				//var endDate = new Date(purgedDateRange.ISO8601.EndDate);

				//InventoryRetrievalParameters.EndDate = endDate.toISOString().split('.')[0]+"Z"; // remove ms
								
				InventoryRetrievalParameters.EndDate = purgedDateRange.ISO8601.EndDate;

				// if(purgedDateRange.ISO8601.StartDate === '') {

				// 	var startDate = new Date(endDate);
				// 	startDate.setDate(endDate.getDate() - 1);
				// 	InventoryRetrievalParameters.StartDate = startDate.toISOString().split('.')[0]+"Z";

				// } else {
				InventoryRetrievalParameters.StartDate = purgedDateRange.ISO8601.StartDate;
				// }

				let params = {
					"accountId": "-", 
					"vaultName": removed,
					"jobParameters": {
						"Description": `Inventory requested on ${new Date()}`,
						"Format": "JSON",
						"Type": "inventory-retrieval"
					}						
				};
				params.jobParameters.InventoryRetrievalParameters = InventoryRetrievalParameters;
				response = await glacier.initiateJob(params);
				if(response) {
					isNewInventoryRequested = true;
					// multiple inventory requests can cause incorrect comparisons
					// when looking for purged documents
					// the job Id will now be saved to all archives within 
					// InventoryRetrievalParameters date range
					updatePurgedArchivesJobId(response.jobId)
					.then(response => {
						console.log(response); // rowCount, returnCode
					})
					return true;
				}
			}
		}
	}
	catch (error) {
		loggerError.logError(`requestNewInventoryJob() -> ${error}`);
		reject(false);
	}
}

/* *********************************************************************************************
 Requests an inventory report from AWS to be parsed the following day
********************************************************************************************* */
const requestArchiveJob = async (docId, archiveId) => {
	try {
		let params = {
			"accountId": "-", 
			"vaultName": removed,
			"jobParameters": {
				"ArchiveId": archiveId,
				"Description": `Archive retrieval for document Id ${docId} on date ${new Date()}`,
				"Type": 'archive-retrieval'
			}						
		};

		response = await glacier.initiateJob(params);
		if(response) {
			//
			// just return, the response is the jobId
			// but we don't need to save it
			//
			return true;
		}
	}
	catch (error) {
		loggerError.logError(`requestArchiveJob() -> ${error}`);
		reject(false);
	}
}

/* *********************************************************************************************
 The inventory data is parsed and compared to local archive data marked PURGED.
 Optional args: inventory. AWS completes an inventory in about 6 hours.
  
 The InventoryDate can be earlier than the CreationDate
 
 InventoryDate
 The UTC date and time of the last inventory for the vault that was completed after changes 
 to the vault. Even though S3 Glacier prepares a vault inventory once a day, the inventory 
 date is only updated if there have been archive additions or deletions to the vault since 
 the last inventory.

 Type: A string representation in the ISO 8601 date format, for example 2013-03-20T17:03:43.221Z
 
 CreationDate
 The UTC date and time the archive was created.

 Type: A string representation in the ISO 8601 date format, for example 2013-03-20T17:03:43.221Z.
********************************************************************************************* */
async function flagDeletes(inventory) {
	try {
		await asyncForEach(awsInventoryListArray, async (element) => {

			if(element.JobId) {

				params = {
					accountId: "-", 
					jobId: element.JobId,
					range: "", 
					vaultName: removed
				};
				// Get inventory output from AWS
				let response = await getInventoryOutputFromAWS(params);
				// send the output inventory data to be parsed
				if(response && response.data && response.data.inventory && response.data.inventory.ArchiveList && response.data.inventory.ArchiveList.length > 0) {
					response = await parseInventory(response.data.inventory, element.CreationDate);
					if(response) {
						console.log(response);
					}
				}
			}
		});

	} catch (error) {
		loggerError.logError(`flagDeletes() -> ${error}`);
		return error;
	}
}

/* *********************************************************************************************
 Any document archive that was deleted in AWS vault and is flagged PURGED will be checked 
 against a list of document archives from AWS vault inventory archives. An archive that does
 not exist in AWS inventory will be flagged as DELETD (6 length restriction).
********************************************************************************************* */
const parseInventory = (inventory, inventory_creation_date) => new Promise((resolve,reject) => {
	//
	// Get archive ids from database
	//

//	loggerError.logError(`inventory.InventoryDate -> ${inventory.InventoryDate}, inventory_creation_date -> ${inventory_creation_date}`);

	buildPurgedDocumentsMap(inventory.InventoryDate, inventory_creation_date)
	.then(response => {		
		if(response && purgedDocumentsMap.size > 0) {
			
			//
			// Compare document ArchiveId to AWS ArchiveId
			//
			// 2-step iteration over purgedDocumentsMap
			// Extract document archiveId from purgedDocumentsMap data map
			// Yield raw values only, no keys
			// 1
			let docs = Array.from(purgedDocumentsMap.values());
			//
			// Extract the archiveId from documents
			// 2
			//let docs_array = docs.map(o => o.archiveId);
			 
			let docs_array = [];
			docs.forEach((value, key, map) => {
				let metadata = JSON.parse([value.metadata]);
				let archiveId = metadata.archive_id;
				if(archiveId && archiveId.length > 0)
					docs_array.push(archiveId);
					
			});

			//
			// extract archiveIds from AWS inventory
			// inventory.ArchiveList.length 0 is Ok
			//
			let aws_array = inventory.ArchiveList.map(o => o.ArchiveId);
			//
			// Do compare work
			// Filter docs_array into new array of items that are not in AWS archive inventory.
			// Items in purgedAWS need to be flagged deleted.
			// Note: Actual deletes will be handled by vault-delete.js
			//
			let purgedAWS  = docs_array.filter(o => !aws_array.some(archiveId => archiveId === o));
			//
			// Add the docId to our new array of items to be deleted
			//  by matched inventory archiveId and document archiveId
			// purgeArray holds docIds to flag deleted
			let purgeArray = [];
			//
			purgedDocumentsMap.forEach(function(doc) {
				purgedAWS.forEach(function(archive) {					

					let metadata = JSON.parse([doc.metadata]);
					let archiveId = metadata.archive_id;
					
					if(archiveId === archive) {
						purgeArray.push(doc.documentId);
					}
				});
			});
			
			// 
			// Do delete flagging on tables
			//
			if(purgeArray.length>0) {

				// TODO: call from single method
				setArchiveDeleted(purgeArray)
				.then(response => {
					// response is the count of affected row updates in table idocs_documents
					let affectedRows = response;
					// same as archive delete but different table
					setDocumentDeleted(purgeArray)
					.then(response => {
						// response is the count of affected row updates in table idocx_field
						// this is just flagging and not a one->many relationship
						// check if the affected rows from both updates do not match.
						if(affectedRows !== response) {		
							// not implemented				
						}
						resolve(true);
					})
				})
				.catch(error => {
					loggerError.logError(`parseInventory().buildPurgedDocumentsMap -> ${error}`);
					reject(error);
				});	

			} else {
				resolve(purgeArray.length);
			}

		} else {
			resolve(response);
		}
	});
});

/* *********************************************************************************************
 Requests the vault inventory JSON data from AWS by the jobId and loads the data into 
 global array. StatusCodes handle expired inventories, and requiring a new inventory request.
 https://docs.aws.amazon.com/amazonglacier/latest/dev/api-error-responses.html
********************************************************************************************* */
const getInventoryOutputFromAWS = (params) => new Promise((resolve,reject) => {
		
	glacier.getJobOutput(params)
	.then(response => {
		var status = (response.status || response.statusCode);
		var r = {"data": "0"};				
		switch (status) {
			case 400: // 400 Bad Request
			case 404: // ResourceNotFoundException
			case 500: // 500 Internal Server Error
				r = {
					"data": response.statusCode
				};
				break;
			case 200:
				if(response.body) {
					let body = Buffer.from(response.body);
					let json = decoder.write(body);
					let inventory = JSON.parse(json);
					r = {
						"data": {
							"inventory": inventory
						}
					}							
				}
				break;
		}
		resolve(r);
	})
	.catch(error => {
		loggerError.logError(`getInventoryOutputFromAWS() -> ${error}`);
		reject(error);
	});
});

/* *********************************************************************************************
	restoreDocument
********************************************************************************************* */
const restoreDocument = (element) => new Promise((resolve,reject) => {

	let params = {
		accountId: "-", 
		jobId: element.JobId,
		range: "", 
		vaultName: removed
	};
	glacier.getJobOutput(params)
	.then(response => {
		var status = (response.status || response.statusCode);
		switch (status) {
			case 400:
			case 404:
				resolve(0);
				break;
			case 200:
				if(response.body) {

					writeRestore(response, element.ArchiveId)
					.then(response => {						
						console.log(response);
						resolve(true);
					})
					.catch(error => {
						loggerError.logError(`restoreDocument() -> writeRestore -> ${error}`);
						reject(error);
					});		 
				}
				break;
		}
	})
	.catch(error => {
		loggerError.logError(`restoreDocument() -> ${error}`);
		reject(error);
	});	

});

/* *********************************************************************************************
 writeRestore
********************************************************************************************* */
const writeRestore = async (output, archiveId) => {

	for await (let [key, values] of restoreJobsMap) {

		let metadata = JSON.parse([values.metadata]);

		if (metadata.archive_id === archiveId) {
		
			let restorePath = path.join(nas.getFolder(values.documentType), 
			values.fileSystemId.substr(0, 2), 
			values.fileSystemId) + '.' + values.fileType;

			// this is correct size in bytes. values.fileSizeInBytes

			let fileSizeInBytes = values.fileSizeInBytes;
			let fileBuffer = output.body;
			
			let checksum = await glacier.computeChecksums(fileBuffer);
			if(checksum && checksum.treeHash === metadata.checksum) {

				try {
					const paoRestorer = new PaoRestorer(restorePath, fileSizeInBytes, fileBuffer);
					//
					// do not exit for until both are resolved
					// wait for the file to be restored
					//
					let response = await paoRestorer.restoreFile();
					if(response) {
						// wait the document record restore flag to be cleared
						response = await setRestoreCompleted(key);
						if(response)
							restoredMap.set(key);
					}

				} catch (error) {
					loggerError.logError(`writeRestore().restoreFile -> ${error}`);
				}
			}
			break;			
		}
	}
}

/* *********************************************************************************************
 getInventoryWork
 Use to include in inventory request for purge date range
********************************************************************************************* */
async function getInventoryWork() {
  try {
		connection = await oracledb.getConnection(
		{
			user: dbConfig.user,
			password: dbConfig.password,
			connectString: dbConfig.connectString
		});
		let sql = `select pkg_purge_services.fnc_get_inv_work cnt from dual`;
		let options = {
			maxRows: 1
		};
    result = await connection.execute(sql, [], options);    		
  } catch (err) {
    console.error(err.message);
  } finally {
    if (connection) {
      try {
        await connection.close();
				return result.rows[0].CNT;
      } catch (err) {
        console.error(err.message);
      }
    }
  }
}

/* *********************************************************************************************
 Run flag deletes and inventory job request
********************************************************************************************* */
const doInventoryDeleteWorkflow = async () => {
	try {
		let response = await flagDeletes();
		if(response) {
			console.log(response);
			return true;
		}	
	} catch (error) {
			loggerError.logError(`doInventoryDeleteWorkflow() -> ${error}`);
		return false;
	}
}

/* ********************************************************************************************* 
  documentWorkflow
  processes documents by procedure
  Loop documents asynchronously
  Procedure types: Upload
	 Upload procedure will add document to AWS Glacier and Idoc vault table
 ********************************************************************************************* */
const requestArchiveRestoreFromAWS = async () => {

	// key: documentId
	// values: { fields }

	let response = '';
	for await (let [key, values] of restoreMap) {

		let metadata = JSON.parse([values.metadata]);
		let archiveId = metadata.archive_id;

		switch (values.procedure) { // this will always be 'Restore'
			case "Restore":
				try {
					let response = await requestArchiveJob(key, archiveId);
					if(response) {
						//
						// update idocx_field to unflag restore request
						//
						response = await setRestoreRequested(key);
						if(response) {
						}	
					}
				} catch (error) {
					loggerError.logError(`requestArchiveRestoreFromAWS() -> ${error}`);
				}
				break;
		}
	}
	return true;
}
	
/* *********************************************************************************************
	Gets previously requested jobs from AWS jobs list
	and extract into appropriate array
	Using CLI: C:>aws glacier list-jobs --account-id - --vault-name removed
	
********************************************************************************************* */
const retrieveGlacierJobList = () => new Promise((resolve,reject) => {
	try {
		params = {
			accountId: "-", 
			vaultName: removed,
		//	statuscode: "Succeeded" // do not filter by statuscode so we can check inventory requests
		};
		glacier.listJobs(params)
		.then(response => {
			if(response.JobList.length>0) {

				//const oneday = 60 * 60 * 24 * 1000;
				var today = new Date();

				// only allow new inventory requests every 6 hours
				// filter inventory requests already done
				// CreationDate format 2020-06-07T22:33:04.133Z
				todaysInventoryRequests = response.JobList.filter(
					o => o.StatusCode !== "Succeeded" && (Math.floor((Math.abs(new Date(o.CreationDate).getTime() - today.getTime()) / 1000) / 3600)) < 7 // hours
				);

				// skip StatusCode InProgress
				awsArchiveListArray = response.JobList.filter(
					o => o.Action === "ArchiveRetrieval" && o.StatusCode === "Succeeded"
				);
				awsInventoryListArray = response.JobList.filter(
					o => o.Action === "InventoryRetrieval" && o.StatusCode === "Succeeded"
				);	

				resolve(true);
			} else {
				resolve(false);
			}
		})
		.catch(error => {
			loggerError.logError(`retrieveGlacierJobList().listJobs -> ${error}`);
			reject(false);
		});
	} catch (error) {
		loggerError.logError(`retrieveGlacierJobList() -> ${error}`);
	}
});

/* *********************************************************************************************
 doInventoryWork
********************************************************************************************* */
async function doInventoryWork() {
	try {
		let response = await getInventoryWork();
		if(response > 0){
			if(awsInventoryListArray.length>0) {
				response = await doInventoryDeleteWorkflow();
				if(response) {
					return true;
				}
			}
		}
	} catch (error) {
		loggerError.logError(`doInventoryWork() -> ${error}`);
		return false;
	}
}

/* *********************************************************************************************
 asyncForEach
********************************************************************************************* */
async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

/* *********************************************************************************************
 doDocumentRestoreWorkflow
********************************************************************************************* */
async function doDocumentRestoreWorkflow() {
	try {
		let response = await getRestoreJobs();
		if(response) {

			// reconciled archive list of restore jobs not completed
			let reconciledArchiveListArray = [];

			restoreJobsMap.forEach(function(job) {
				awsArchiveListArray.forEach(function(archive) {

					let metadata = JSON.parse([job.metadata]);
					let archiveId = metadata.archive_id;

					if(archiveId === archive.ArchiveId) {
						reconciledArchiveListArray.push(archive);
					}
				});
			});
 
			await asyncForEach(reconciledArchiveListArray, async (element) => {
				if(element.JobId) {
					await restoreDocument(element);
				}
			});
		}
	} catch (error) {
		loggerError.logError(`doDocumentRestoreWorkflow() -> ${error}`);
		return false;
	}
	finally {
		return true;
  }	
}

/* *********************************************************************************************
 doRestoreWork
 awsArchiveListArray, AWS always fetches the archive in an archive-retrieve job
********************************************************************************************* */
const doRestoreWork = async () => {
	try {
		// check for any new restores to do
		let response = await getRestoreDocuments();
		if(response) {
			if(awsArchiveListArray.length > 0) { // has archive and ready to download
				if(restoreMap.size > 0) {
					response = await requestArchiveRestoreFromAWS();
					if(response>0) {
						console.log(response);
					}		
				}
			}
		}		
		response = await doDocumentRestoreWorkflow();
		if(response) {
			console.log(response);
		}
	} catch (error) {
		console.error(error);
		return false;
	}
	finally {
  	return true;
  }
}

/* *********************************************************************************************
 run
********************************************************************************************* */
async function run() {

	let promise = new Promise((resolve, reject) => {
		
		setPurgedDateRangeObject()
		.then(retrieveGlacierJobList)
		.then(doInventoryWork)
		.then(requestNewInventoryJob)
		.then(doRestoreWork)
		.then(response => {
			resolve(response);
		})
		.catch(error => {
			loggerError.logError(`run() -> ${error}`);
			reject(false);
		});

	});
	let result = await promise;
	return result;
}

/* *********************************************************************************************
	validateNasPath()
	return true = valid path
********************************************************************************************* */
function validateNasPath() {

		var d = nas.documentsFolder();
		var p = nas.photosFolder();

		if (!fs.existsSync(d)) {
			loggerError.logError(`Folder does not exist: ${d}`);
			return false;
		}
		if (!fs.existsSync(p)) {
			loggerError.logError(`Folder does not exist: ${p}`);
			return false;
		}
		// nas paths are valid
		return true;
}

/* ***********************************************************
* Start here
* 
* Semantics:
* 
************************************************************ */

/* *************************************
	frame logging properties
************************************* */

// where to write log
let FilePath		= 'xxxxxxxxxxxx';
let FileName		= `glacier-inventory`; // log file name prefixed to timstamp in loggerError

let logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: isDebugMode
}
const loggerError = new PaoLogger(logProps);

//
// purgedDateRange
//
// Available Date Time Formats: 
// ISO-8601: 	YYYY-MM-DD"T"HH24:MI:SS"Z"
// Ex. 				2020-04-21T17:57:54Z
// UTC: 			MM-DD-YYYY HH24:MI:SS
// Ex.				04-21-2020 14:43:28
// ORACLE NLS_DATE_FORMAT
// DD-MON-RR HH24:MI:SS
// Or, DD-MON-YY HH24:MI:SS
// Ex. 21-APR-20 17:29:52
// 2021-01-20, gwb, changed from const
let purgedDateRange = {
	ISO8601: {
		EndDate: 0,
		StartDate: 0
	},
	UTC: {
		EndDate: 0,
		StartDate: 0
	},
	NLS: {
		EndDate: 0,
		StartDate: 0		
	}
};

// all documents marked PUREGED in table idocs_documents
let purgedDocumentsMap = new Map();
let restoreMap = new Map();
let restoreJobsMap = new Map();
let restoredMap = new Map();
let isNewInventoryRequested = false;

// job lists
let awsInventoryListArray = [];
let awsArchiveListArray = [];
let todaysInventoryRequests = [];
 
if(1===1) {

	new PaoGlacier(removed)
  .then(response => {

		// set glacier object
		glacier = response;

		// get runtime documents folder
	  nas.nasfileserver()
		.then(response => {
			return validateNasPath();
		})
		.then(response => {
			if(response) {
				return run();
			} else {
				return false;
			}
		})
		.then(response => {
			var lastjr = "";
			if(todaysInventoryRequests[0]?.CreationDate) {
				lastjr = `\tLast Job Request: ${todaysInventoryRequests[0].CreationDate}.`;
			}
			var msg = `\tInventory completed: ${response}.${lastjr}\tProcessed: ${purgedDocumentsMap.size}.\tRestores: ${restoredMap.size}.\tErrors: ${loggerError.errorCount()}.`;

			console.log(msg);
		
			if(process.send) {
				process.send(msg);
				process.exit(1);
			}
			return response;
		})
		.catch(error => {
			console.error(error);
		});	
	})
}
// *************************************************
