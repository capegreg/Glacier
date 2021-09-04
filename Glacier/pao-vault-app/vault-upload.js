/***
	Glacier \ Amazon S3 Glacier
	Created by: GBologna
	Created On: 3/18/2020
	Git: repos\Glacier

	-> Handles all document uploads to AWS S3 Glacier
	-> Tables IDOCS.idocs_documents 
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
	
	TODO: find a better way to return results and not use fetch max rows

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
// logging class
const PaoLogger = require('./PaoLogger');
// file handling
const path = require('path');
const fs = require('fs');
// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

// nas is global
const nas = new PaoNas();

/* *********************************************************************************************
	getFileLocationOnDisk
	obj lit., values {fileSystemId, fileType}
	return path to file if file exists
********************************************************************************************* */
const getFileLocationOnDisk = (key, values) => {

	var file = path.join(nas.getFolder(values.documentType), values.fileSystemId.substr(0, 2), values.fileSystemId) + '.' + values.fileType;
	if (fs.existsSync(file)) {
		try {
			// can read
			fs.accessSync(file, fs.constants.R_OK);
			return file;
		} catch (error) {		
			loggerError.logError(`Read permission denied -> Id: ${key}-${file}`);			
			return 'ACCESS ERROR';
		}
	} else {
		loggerError.logError(`File not found -> Id: ${key}-${file}`);
		return 'FILE NOT FOUND';	
	}
}

/* *********************************************************************************************
  addDocumentToArchive
  obj lit., values {fileSystemId, fileType}
 ********************************************************************************************* */
async function addDocumentToArchive(key, values) {

	let promise = new Promise((resolve, reject) => {
		
		var file = getFileLocationOnDisk(key, values);
		switch (file) {
			case 'ACCESS ERROR':
					// just resolve, already logged
					resolve(false);
				break;
			case 'FILE NOT FOUND':
					// file does not exist, flag to delete record
					flagDeleteDocument(key, 'DELETD_OS2');
					let params = {};
					params['docId'] = key;
					params['scenario'] = 'FILE NOT FOUND FOR UPLOAD';
					loggerOrphan.logOrphan(params);
					resolve(false);
				break;
			default:
				fs.stat(file, (err, stats) => {
					if (err) {
						console.error(err)
						reject(error);
					}
					//we have access to the file stats in `stats`
					glacier.uploadArchive(key, file, stats.blksize)
					.then(response => {
						var doc = {	
							archiveId:	response.archiveId,
							checksum: response.checksum,
							size: stats.size			
						}					
						resolve(doc);
					})
					.catch(error => {
						loggerError.logError(`addDocumentToArchive() -> ${key}-${error}`);
						reject(error);
					});	
				});
			} // end switch
	});
	let result = await promise;
	return result;
}

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
		  "BEGIN pkg_upload_services.prc_get_docs_to_upload(:cursor); END;",
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
		loggerError.logError(`getDocuments() -> ${error}`);
		reject(false);
	});
});

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

/* *********************************************************************************************
	fetchRowsFromRS
	populates vaultMap data object with documents to process
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

				vaultMap.set(row.DOCUMENTID, {
					"procedure": row.PROCEDURE,
					"parid": row.PARID, 
					"filename": row.FILENAME,
					"fileSystemId": row.FILESYSTEMID, 
					"fileType": row.FILETYPE,
					"documentType": row.DOCUMENTTYPE
				});

			});
			resultSet.close();
			resolve(rows.length);
		}
	});
});

/* *********************************************************************************************
	addArchiveIdocsVaultTable
	params: obj. lit.
		v_parid: archive.parid
		v_document_id: archive.documentId
		v_filename: archive.filename
		v_file_type: archive.fileType
		v_metadata: archive.archiveId

	Note: timestamp will be saved to JUR in procedure. This is needed for date range purge inventories

	Table: IDOCS.IDOCS_DOCUMENTS
		This was a cannibilized table. The fields that are not null and not used have been set to null 
		or 0. blob field is set to EMPTY_BLOB() which is 0 length
********************************************************************************************* */
const addArchiveIdocsVaultTable = (archive) => new Promise((resolve, reject) => {

	oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
	oracledb.autoCommit = true;
	oracledb.extendedMetaData = false;
	oracledb.getConnection(
	{
		user : dbConfig.user,
		password : dbConfig.password,
		connectString : dbConfig.connectString
	})
	.then(connection => {
		var bindvars = {	
			v_parid: archive.parid, 						// not null
			v_document_id: archive.documentId, 	// not null
			v_filename: archive.filename, 			// not null
			v_filesize: archive.filesize,
			v_file_type: archive.fileType, 			// specifies the current job type: VARCHAR2(6) UPLOAD, PURGE, RESTOR
			v_metadata: archive.archiveId, 			// AWS archive Id	
			v_checksum: archive.checksum,		
			rowcount:  		{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
			return_code: 	{ type: oracledb.NUMBER, dir: oracledb.BIND_OUT },
		}
		const result = connection.execute(
			"BEGIN pkg_upload_services.prc_ins_archive(:v_parid, :v_document_id, :v_filename, :v_filesize, :v_file_type, :v_metadata, :v_checksum, :rowcount, :return_code); END;",
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
			loggerError.logError(`addArchiveIdocsVaultTable().response -> ${error}`);
			reject(false);
		});		
	})
	.catch(function(error) {
		loggerError.logError(`addArchiveIdocsVaultTable() -> ${error}`);
		reject(error);
	});

});

/* ************************************** 
 * documentWorkflow
 * processes documents by procedure
 * 
 * Loop documents asynchronously
 * 
 * Procedure types: Upload
 *  Upload procedure will add document 
 *   to AWS Glacier and Idoc vault table
 ************************************** */
const documentWorkflow = async () => {

	// key: documentId
	// values: { fields }

	let response = '';

	for await (let [key, values] of vaultMap) {
		
		switch (values.procedure) { // this will always be 'upload'
			case "Upload":
				try {
					// Step 1. Add document to glacier and check for a required archive Id
					// values {fileSystemId, fileType}
					response = await addDocumentToArchive(key, values);
					if(response && response !== false) {
						// prepare data for table insert
						let archive = {
							parid: values.parid,
							documentId: key,
							filename: values.filename,
							filesize: response.size,
							fileType: 'ARCHVD',
							archiveId: response.archiveId,
							checksum: response.checksum
						};
						// Step 2. Add archive Id to database table
						response = await addArchiveIdocsVaultTable(archive);
						if(response) {
							var msg = `Document Id ${key} archive ${(response.returnCode === 0 ? 'successful' : 'failed to add record to table')}.`;
							console.log(msg);
						}		
					}
				} catch (error) {
					loggerError.logError(`documentWorkflow() -> Id: ${key}-${error}`);
				}
				break;
		}
	}
	return true;
}

/* *********************************************************************************************
	doWorkflow handles the archiving process
********************************************************************************************* */
const doArchiveWorkflow = () => new Promise((resolve, reject) => {

	// do new archive upload work
	getDocuments()
	.then(response => {
		if(response > 0 && vaultMap.size > 0) {
			documentWorkflow()
			.then(response => {					
				resolve(response);
			})
			.catch(error => {
				loggerError.logError(`doArchiveWorkflow().documentWorkflow() -> ${error}`);
				reject(false);
			});
		} else {
			resolve('No documents');
		}
	})
	.catch(error => {		
		loggerError.logError(`doArchiveWorkflow() -> ${error}`);
		reject(false);
	});
	
});

/* *********************************************************************************************
	vault-upload starts here
********************************************************************************************* */
async function run() {

	let promise = new Promise((resolve, reject) => {
				
		doArchiveWorkflow()
		.then(response => {
			resolve(response);
		})
		.catch(error => {			
			loggerError.logError(`run(): ${error}`);
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
* Add globals here
* 
* Semantics:
* 
************************************************************ */

/* *************************************
 frame logging properties
************************************* */

// where to write log
let FilePath		= '\\\\manateepao.com\\MCPAO_Data\\IT\\Source_Code\\GlacierVault\\glacier_logs';
let FileName		= `glacier-upload-debug`; // (suffixed to timestamp in logger)
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
let vaultMap = new Map();

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
			var msg = "0";
			if(response !== "No documents") {
				msg = `\tUpload completed: ${response}.\tDocuments: ${vaultMap.size}.\tErrors: ${loggerError.errorCount()}.\tOrphans: ${loggerOrphan.orphanCount()}.`;
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
	})

}
// *************************************************