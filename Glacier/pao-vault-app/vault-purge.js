/***
	Glacier \ Amazon S3 Glacier
	Created by: GBologna
	Created On: 3/18/2020
	Git: repos\Glacier

	Handles all document purges in AWS S3 Glacier 
		and update tables. Tables IDOCS.idocs_documents 
		and IDOCS.idocx_field	are considered the archive 
		table and the document tables respectivly

 DOCUMENT TERMS
	1. ARCHVD: Flag indicating that a document was archived
	2. PENDING PURGE: Flag indicating that a document can be deleted from AWS and disk
	3. PURGED: Flag indicating that a document was deleted from AWS and is awaiting audit 
		for purging in the inventory process
	4. DELETD: Flag indicating that a document was audited in the inventory process and is 
		shall be purged in the delete process
	5. PENDING RESTORE: Flag indicating that a request has been made for an archive restore from AWS
	6. RESTORE REQUESTED: Flag indicating that an archive retrieval job was initiated per a 
		PENDING RESTORE request
	7. Special system delete flags include: DELETD_NS2 & DELETD_OS2
 */

const isDebugMode = process.env.DEBUG_MODE;

// the AWS vault where all archives are kept
const removed = 'removed';
// rest api
const PaoGlacier = require('./PaoGlacier');
// PaoGlacier object
let glacier = undefined;
// logging class
const PaoLogger = require('./PaoLogger');
// file handling
const path = require('path');
const fs = require('fs');
// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

let purgeOtherCount 	= 0;
let purgeGlacierCount = 0;

/* *************************************************************************************
  purgeArchive
  params: 
		docId
		archiveId
 ********************************************************************************************* */
const purgeArchive = (docId, archiveId) => new Promise((resolve, reject) => {

	// delete archive in glacier
	var params = {
		accountId: "-", 
		archiveId: archiveId,
		vaultName: removed
	};
	glacier.deleteArchive(params)
	.then(response => {
		
		if(response && response.length> 0) {
			// log response from AWS
			loggerError.logError(`glacier.deleteArchive -> ${response} -> Id: ${docId}`);
			reject(false);
			
		} else {
			
			// AWS archive was deleted successfully
		
			// Now set flag purged on archive and document field tables
			setArchiveTablePurgeInProgress(docId)
			.then(response => {
				if(response.rowCount !== 2) {
					// check return_code
					switch (response.returnCode) {
						case 9: 	// rollback occurred
						case 1403: 	// no data found				
							resolve(false);
							break;
						default:
							resolve(true);
					}
				} else if(response.rowCount === 2) {
					resolve(true);
				} else {
					resolve(false);
				}
			})
			.catch(error => {
				loggerError.logError(error);
				reject(error.code);
			})	
		}
	})
	.catch(error => {
		loggerError.logError(`setArchiveTablePurgeInProgress() -> Id: ${docId}, ${error}`);
		reject(error.code);
	});
});

/* *********************************************************************************************
  getDocuments
 ********************************************************************************************* */
const getDocuments = () => new Promise((resolve, reject) => {

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
		  "BEGIN pkg_purge_services.prc_get_docs_to_purge(:cursor); END;", 
			bindvars,
    )
		.then(result => {
			fetchRowsFromRS(connection, result.outBinds.cursor, 100000)
			.then(result => {
				connection.close();
				resolve(true);
			});
		})
		.catch( error => {			
			connection.close();
			loggerError.logError(`getDocuments().fetchRowsFromRS() -> ${error}`);
			reject(false);
		});		
	})
	.catch(function(error) {
		loggerError.logError(`getDocuments() -> ${error}`);
	});
});

/* *********************************************************************************************
	fetchRowsFromRS
	populates purgeMap data object with documents to process
********************************************************************************************* */
const fetchRowsFromRS = (connection, resultSet, numRows) => new Promise((resolve, reject) => {
	resultSet.getRows(// get numRows rows
		numRows,			
		function (error, rows) {
		if (error) {
			resultSet.close();
			loggerError.logError(`fetchRowsFromRS().resultSet -> ${error}`);
			reject(error);
		} else if (rows.length == 0) {  // no rows, or no more rows
			resultSet.close();
			resolve(0);
		} else if (rows.length > 0) {
			rows.forEach(function(row) {
				purgeMap.set(
					row.DOCUMENTID, {
						"procedure": row.PROCEDURE, 
						"documentId": row.DOCUMENTID, 
						"metadata": row.METADATA
					});
			});
			resultSet.close();
			resolve(rows.length);
		}
	});
});

/* *********************************************************************************************
	setArchiveTablePurgeInProgress

	update tables IDOCS.idocs_documents and IDOCS.idocx_field with flag to indicate that the
	archive was purged from AWS Glacier. The physical and logical document deletions will be 
	processed by vault-inventory.js

  PURGED, the document was deleted from AWS and is awaiting audit for deletion in the inventory process

	Returns result object
		result.outBinds.rowcount 0 error 2 success
		result.outBinds.return_code 9 rollback 0 success
********************************************************************************************* */
const setArchiveTablePurgeInProgress = (docId) => new Promise((resolve, reject) => {

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
			rowcount:  		{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			return_code: 	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			id: docId
		}
		const result = connection.execute(
			"BEGIN pkg_purge_services.prc_upd_doc_purged(:rowcount, :return_code, :id); END;",
			bindvars,
		)
		.then(result => {
			
			connection.close();

			if(result.outBinds.return_code === 9) {
				// log rollback
				loggerError.logError(`setArchiveTablePurgeInProgress().response -> ${result.outBinds.return_code}`);
			}

			var response = {
				rowCount: result.outBinds.rowcount,
				returnCode: result.outBinds.return_code
			};
			
			resolve(response);

		})
		.catch( error => {			
			connection.close();
			loggerError.logError(`setArchiveTablePurgeInProgress().response -> ${error}`);
			reject(false);
		});		
	})
	.catch(function(error) {
		loggerError.logError(`getDocuments() -> ${error}`);
		reject(error);
	});
});

/* *********************************************************************************************
	flagDeleteDocument 
	********************************************************************************************* */
const flagDeleteDocument = (docId, flagString) => new Promise((resolve, reject) => {

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
			rowcount:  		{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			return_code: 	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			id: docId,
			flag: flagString
		}
		const result = connection.execute(
			"BEGIN pkg_purge_services.prc_upd_del_doc(:rowcount, :return_code, :id, :flag); END;",
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
			loggerError.logError(`deleteDocument().result -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`deleteDocument() -> ${error}`);
		reject(error);
	});
});


/* ************************************** 
 * documentWorkflow
 * processes documents by procedure
 * Loop documents asynchronously
 * Procedure types: Purge
 ************************************** */
const documentWorkflow = async () => {

	// key: documentId
	// values: { procedure, documentId, archiveId }

	for await (let [key, values] of purgeMap) {

		let metadata = '';
		let archiveId = '';

		try {
				metadata = JSON.parse([values.metadata]);
				archiveId = metadata.archive_id;
		} catch (e) {
				// do nothing
		}
		
		switch (values.procedure) {
			// Purge the document from AWS
			case "PurgeNormalScenarioOne":
			case "PurgeOrphanScenarioOne":
				try {
					if(archiveId.length>0) {
						let response = await purgeArchive(key, archiveId);						
						if(response === true) {
							purgeGlacierCount++;
						}
					}					
				} catch (error) {

					switch (error) {
						case 'ResourceNotFoundException':
							// logged in purgeArchive
							// this might happen if you are testing in AWS dev vault
							// after iaswdev was refreshed from production
							break;
						default:
							loggerError.logError(`documentWorkflow() -> Id: ${key}-${error}`);
					}

				}					
				break;
				
			case "PurgeNormalScenarioTwo":
					let response = await flagDeleteDocument(key, 'DELETD_NS2');
					if(response?.rowCount>0) {
						// do nothing
						// log orphan in deleted job
						purgeOtherCount++;						
					}
				break;
		}		
	}
	return true;
}

/* *********************************************************************************************
	doPurgingWorkflow handles the purging process
********************************************************************************************* */
const doPurgeWorkflow = () => new Promise((resolve, reject) => {

	// Get documents to purge and update purge status
	getDocuments()
	.then(response => {
		if(response > 0 && purgeMap.size > 0) {
			documentWorkflow()
			.then(response => {	
				resolve(response);
			});
		} else {
			resolve(response);
		}
	})
	.catch(error => {	
		loggerError.logError(error);	
		reject(false);
	});
});

/* *********************************************************************************************
	Purging occurs in AWS Glacier. Local documents are deleted in document-delete.js
********************************************************************************************* */
async function run() {

	let promise = new Promise((resolve, reject) => {

		doPurgeWorkflow()
		.then(response => {
			resolve(response);
		})
		.catch( error => {
			loggerError.logError(`run(): ${error}`);
			reject(false);
		});

	});
	let result = await promise;
	return result;
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
let FileName		= `glacier-purge`; // log file name prefixed to timstamp in logger

let logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: isDebugMode
}
const loggerError = new PaoLogger(logProps);

// new logger for orphans
FileName		= `glacier-orphans`;
logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: isDebugMode
}
const loggerOrphan = new PaoLogger(logProps);

// all document data consisting of uploads, purge, 
// purge in progress, and restores
let purgeMap = new Map();

if(1===1) {

 new PaoGlacier(removed)
  .then(response => {
		glacier = response;
	})
	.then(run)
	.then(response => {
		
		var msg = "0";

		if(response !== true || purgeMap.size + purgeGlacierCount + purgeOtherCount + loggerError.errorCount() > 0) {
			msg = `\tPurge completed: ${response}.\tDocuments: ${purgeMap.size}.\tDocuments purged in AWS: ${purgeGlacierCount}.\tAll others: ${purgeOtherCount}.\tErrors: ${loggerError.errorCount()}.`;
		}

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

}
// *************************************************
