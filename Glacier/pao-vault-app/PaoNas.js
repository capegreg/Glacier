/*
 * PaoNas
 * GBologna
 * Provides the nas document location path
 */

// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

let m_parent_fileserver = "";

class PaoNas {

	constructor() {}
	
	/* *********************************************************************************************
		property, parentFileserver
			public
		 	return path to nas documents folder
			usage: PaoNas.parentFileserver;
			caller: nas.documentsfolder()
	********************************************************************************************* */
	static get parentFileserver() { return m_parent_fileserver; }
	static set parentFileserver(value) { m_parent_fileserver=value; }
	
	documentsFolder = () => {
		return `${PaoNas.parentFileserver}\\Docs`;
	};
	photosFolder = () => {
		return `${PaoNas.parentFileserver}\\Photos`;
	};

	/* *********************************************************************************************
	getFolder
	param: documentType {document, photo}
	return: variable to document disk location
	********************************************************************************************* */
	getFolder(documentType) {
		switch (documentType) {
			case 'document':
				return this.documentsFolder();
				break;
				case 'photo':
					return this.photosFolder();
				break;
		}
	}

	/* *********************************************************************************************
	 getParentFileserverPathFromDatabase
	 function fnc_get_nas_path returns the path to the nas documents
	********************************************************************************************* */
	getParentFileserverPathFromDatabase = () => new Promise((resolve, reject) => {
		oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
		oracledb.extendedMetaData = false;
		oracledb.getConnection(
		{
			user : dbConfig.user,
			password : dbConfig.password,
			connectString : dbConfig.connectString
		})
		.then(connection => {
			let sql = `select pkg_utils.fnc_get_nas_path from dual`;
			let options = {
				maxRows: 1
			};
			const result = connection.execute(
				sql, [], options
			)
			.then(result => {
				connection.close();
				resolve(result);
			});
		})
		.catch(error => {			
			connection.close();
			logger.logError(`getParentFileserverPathFromDatabase() -> ${error}`);
			reject(false);
		});
	});

	/* *********************************************************************************************
	 nasfileserver
	 Use Windows-style path
	********************************************************************************************* */
	nasfileserver() {
		return new Promise((resolve, reject) => {
			this.getParentFileserverPathFromDatabase()
			.then(response => {
				let p = response;
				PaoNas.parentFileserver = p.rows[0]['FNC_GET_NAS_PATH'];
				resolve(true);
			})
			.catch(error => {
				console.error(error);
			})
		});
	}
} // end class

module.exports = PaoNas;