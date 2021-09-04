const AWS = require('aws-sdk');
// Database packages
const oracledb = require('oracledb');
const dbConfig = require('./dbconfig.js');

class PaoCreds {

	constructor() {
		/**
		 * Connection settings need to be changed in four files. The first two are for CLI.
		 * 1. C:\Users\<name>\.aws\credentials
		 * 2. C:\Users\<name>\.aws\config
		 * 3. dbconfig.js
		 * 4. Here
		 * Prod	removed
		 * Dev removed
		 */
		this.region = '';
	}

	/* *********************************************************************************************
	 getCredentialsFromDatabase
	 function fnc_get_aws_settings returns FNC_GET_AWS_SETTINGS of a json string
	********************************************************************************************* */
	getCredentialsFromDatabase = () => new Promise((resolve, reject) => {
		oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
		oracledb.extendedMetaData = false;
		oracledb.getConnection(
		{
			user : dbConfig.user,
			password : dbConfig.password,
			connectString : dbConfig.connectString
		})
		.then(connection => {
			let sql = `select pkg_utils.fnc_get_aws_settings from dual`;
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
			logger.logError(`getCredentialsFromDatabase() -> ${error}`);
			reject(false);
		});
	});

	/* *********************************************************************************************
	 authenticate
	 this must resolve before making requests to AWS
	********************************************************************************************* */
	authenticate() {
		return new Promise((resolve, reject) => {
			this.getCredentialsFromDatabase()
			.then(response => {
				let credentials = response;
				let json = JSON.parse(credentials.rows[0]['FNC_GET_AWS_SETTINGS']);
				var creds = new AWS.Credentials(json);	
				AWS.credentials = creds;
				var config = new AWS.Config(json);	
				AWS.config = config;
				AWS.config.update({region: this.region});	
				resolve(new AWS.Glacier({apiVersion: '2012-06-01'}));
			})
			.catch(error => {
				console.error(error);
			})
		});
	}
} // end class

module.exports = PaoCreds;
