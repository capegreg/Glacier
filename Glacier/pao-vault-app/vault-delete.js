/***
	File name: vault-delete.js
	Glacier \ Amazon S3 Glacier
	Created by: GBologna
	Created On: 3/18/2020
	Git: repos\Glacier

	-> Handles all document purges in AWS S3 Glacier 
		and update tables. 

	-> Tables IDOCS.idocs_documents 
			and IDOCS.idocx_field	are considered the archive 
			table and the document tables respectivly

	-> Does not use AWS

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

 // TODO: find a better way to return results and not use fetch max rows

const isDebugMode = process.env.DEBUG_MODE;

// the AWS vault where all archives are kept
const removed = 'removed';
// error logging
const PaoLogger = require('./PaoLogger');
// PaoNas nas parent location
const PaoNas = require('./PaoNas');
// file handling
const path = require('path');
const fs = require('fs');
// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

// nas is global
const nas = new PaoNas();

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
		var bindvars = {	
			cursor:  { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
		}
		// The PL/SQL has an OUT bind of type SYS_REFCURSOR
		const result = connection.execute(
		  "BEGIN pkg_purge_services.prc_get_docs_to_del(:cursor); END;",
			bindvars,
    )
		.then(result => {
			
			fetchRowsFromRS(connection, result.outBinds.cursor, 1000)
			.then(result => {
				connection.close();
				resolve(true);
			});

		})
		.catch(error => {			
			connection.close();
			loggerError.logError(`getDocuments().fetchRowsFromRS() -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`getDocuments().fetchRowsFromRS() -> ${error}`);
	});
});

/* *********************************************************************************************
	fetchRowsFromRS
	populates deleteMap data object with documents to process
********************************************************************************************* */
const fetchRowsFromRS = (connection, resultSet, numRows) => new Promise((resolve, reject) => {
	resultSet.getRows(// get numRows rows
		numRows,			
		function (error, rows) {
		if (error) {
			resultSet.close();
			loggerError.logError(`fetchRowsFromRS() -> ${error}`);
			reject(error);
		} else if (rows.length == 0) {  // no rows, or no more rows
			resultSet.close();
			resolve(0);
		} else if (rows.length > 0) {
			rows.forEach(function(row) {

				let metadata = '';
				let fileSystemId = '';
				let archiveId = '';

				try {
						metadata = JSON.parse([row.METADATA]);
						fileSystemId = metadata.filesystem_id;
						archiveId = metadata.archive_id;
				} catch (e) {
						// do nothing
				}

				deleteMap.set(
					row.DOCUMENTID, {
						"procedure": row.PROCEDURE, 
						"documentId": row.DOCUMENTID, 
						"fileSystemId": fileSystemId,
						"documentType": row.DOCUMENTTYPE,
						// "hasThumbnail": row.HASTHUMBNAIL,
						"fileType": row.FILETYPE,
						"archiveId": archiveId
					});

			});
			resultSet.close();
			resolve(rows.length);
		}
	});
});

/* *********************************************************************************************
	deleteDocumentFromTables

	Delete document from tables 

	idocs.idocx_field
	idocs.idocx_document
	idocs.idocs_documents

	Returns result object
		result.outBinds.rowcount 0 error 2 success
		result.outBinds.return_code 9 rollback 0 success
********************************************************************************************* */
const deleteDocumentFromTables = (docId) => new Promise((resolve, reject) => {

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
			"BEGIN pkg_purge_services.prc_del_docs(:rowcount, :return_code, :id); END;",
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
			loggerError.logError(`deleteDocumentFromTables().result -> ${error}`);
			reject(false);
		});		
	})
	.catch(error => {
		loggerError.logError(`deleteDocumentFromTables() -> ${error}`);
		reject(error);
	});
});

/* *********************************************************************************************
	getFileLocationOnDisk
	obj lit., values {fileSystemId, fileType}
	return path to file if file exists
********************************************************************************************* */
const getFileLocationOnDisk = (values) => {

	var file = path.join(nas.getFolder(values.documentType), values.fileSystemId.substr(0, 2), values.fileSystemId) + '.' + values.fileType;
	if (fs.existsSync(file)) {
		try {
			// can read
			fs.accessSync(file, fs.constants.R_OK);
			return file;
		} catch (error) {		
			loggerError.logError(`Read permission denied -> ${file}`);
			return error;
		}
	} else {
		loggerError.logError(`File not found -> ${file}`);
		return false;
	}
}

/* *********************************************************************************************
 deleteDocumentsFromDisk
  delete the documment
  check if images have a thumbnail and delete it
********************************************************************************************* */
const deleteDocumentsFromDisk = (values) => new Promise((resolve, reject) => {
	try {
		var file = getFileLocationOnDisk(values);
		if(file !== false) {
			fs.unlinkSync(file);
			resolve(values.documentId);
		
			// 2021-02-24, gwb, changed to delete by file name
			// thumbnail_content is not available for all rows of document
			try {

				//if(values.hasThumbnail === "true") {

					var folder = path.dirname(file);
					var basename = path.basename(file, `.${values.fileType}`);
					file = `${basename}-th.${values.fileType}`;
					file = path.join(`${folder}\\${file}`);

					if (fs.existsSync(file)) {
						fs.unlinkSync(file);
						resolve(values.documentId);
					}	

			} catch (error) {
				// do nothing
			}

			//} // end delete thumbnail
		} // end delete file
	} catch (error) {
		loggerError.logError(`deleteDocumentsFromDisk() -> ${error}`);
		reject(error);
	}
});

/* *********************************************************************************************
	documentWorkflow
 		processes documents by procedure
 	Loop documents asynchronously
 	Procedure types: Deletes, Orphans
		Deletes, Orphans
	 		delete table records where exists
		Deletes
			unlink file	 	
********************************************************************************************* */
const documentWorkflow = async () => {

	// key: documentId
	// values: { procedure, documentId, etc }

	// ORPHAN SCENERIO #2 – Document imaging system write failure 
	// a.	Determined by the existence of a record in IDOCX_DOCUMENT and no corresponding record in IDOCS_DOCUMENTS and no file on disk
	// b.	Delete document metadata
	// c.	Add line item to deletion report
	// d. 2021-01-22, gwb, added DeleteOrphanScenarioOne
	
	for await (let [key, values] of deleteMap) {
		//
		// delete document from tables and disks when exists
		//

		let params = {};

		params['docId'] = key;
		params["archiveId"] = values.archiveId = "" ? "null" : values.archiveId;
		
		switch (values.procedure) {
			case "DeleteNormalScenarioOne":				
			case "DeleteNormalScenarioTwo":
			case "DeleteOrphanScenarioOne":
			case "DeleteOrphanScenarioTwo":	
				try {
					let response = await deleteDocumentFromTables(key);
					if(response) { 	// add any extra handling here
						
						params['returnCode'] = response.returnCode
						params['scenario'] = values.procedure;

						switch (response.returnCode) {
							case 3: 		// no rows affected
							case 9: 		// rollback occurred
								break;
							default:		// otherwise delete file on disk
								switch (values.procedure) {
									case "DeleteNormalScenarioOne":
									case "DeleteNormalScenarioTwo":
										response = await deleteDocumentsFromDisk(values);
										loggerDelete.logDeleted(params);
										break;
								}		
								switch (values.procedure) {
									case "DeleteOrphanScenarioOne":
									case "DeleteOrphanScenarioTwo":
										loggerOrphan.logOrphan(params);
										break;
								}														
						}
					}
				} catch (error) {
					loggerError.logError(`documentWorkflow() -> delete document record -> ${error}`);
				}
				break;
		}
	}
	return true;
}

/* *********************************************************************************************
 doWorkflow
********************************************************************************************* */
const doWorkflow = () => new Promise((resolve, reject) => {

	// Get documents to delete
	getDocuments()
	.then(response => {
		if(response > 0 && deleteMap.size > 0) {
			documentWorkflow()
			.then(response => {	
				resolve(response);
			});
		} else {
			resolve(response);
		}
	})
	.catch(error => {
		loggerError.logError(`doWorkflow() -> ${error}`);
		reject(false);
	});
});

/* *********************************************************************************************
	Purging occurs in AWS Glacier. Local documents are deleted in document-delete.js
********************************************************************************************* */
async function run() {

	let promise = new Promise((resolve, reject) => {

		doWorkflow()
		.then(response => {
			resolve(response);
		})
		.catch( error => {
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
// frame logging properties
************************************* */

// where to write  log
let FilePath		= `\\\\manateepao.com\\MCPAO_Data\\IT\\Source_Code\\GlacierVault\\glacier_logs`;
let FileName		= `glacier-delete-errors`; // log file name prefixed to timstamp in logger

let logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: isDebugMode
}
const loggerError = new PaoLogger(logProps);

// new logger for deletes
FileName		= `glacier-deletes`;
logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: isDebugMode
}
const loggerDelete = new PaoLogger(logProps);

// new logger for orphans
FileName		= `glacier-orphans`;
logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: isDebugMode
}
const loggerOrphan = new PaoLogger(logProps);

// collection of errors for logs
var _error_summary = [];
var _purgeErrors = [];
var _archive_deleted = [];

// all document data consisting of uploads, purge, 
// purge in progress, and restores
let deleteMap = new Map();

if(1===1) {

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
		var msg = "0";
		if(response !== true || deleteMap.size + loggerError.errorCount() + loggerDelete.deleteCount() + loggerOrphan.orphanCount() > 0) {
			msg = `\tDelete completed: ${response}.\tDocuments: ${deleteMap.size}.\tErrors: ${loggerError.errorCount()}.\tDeletes: ${loggerDelete.deleteCount()}.\tOrphans: ${loggerOrphan.orphanCount()}.`;
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