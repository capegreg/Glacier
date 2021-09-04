const PaoCreds = require('./glacier');
const fs = require('fs');
let glacier = undefined;

/* *********************************************************************************************
 PaoGlacier class initializes AWS credentials with each singleton
********************************************************************************************* */
class PaoGlacier extends PaoCreds {
	constructor(vaultName) {
	 	//super(vaultName);
		 super();
		 this.vaultName = vaultName;
		return new Promise((resolve, reject) => {
			super.authenticate()
			.then(response => {
				//this.glacier = response;
				glacier = response;
				resolve(this);
			})
      .catch(error => {
				console.error(error);
			})
    });
	}

		/* *********************************************************************************************
			describeVault(params = {}, callback) ⇒ AWS.Request

			https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#describeVault-property

			data = {
				CreationDate: "2016-09-23T19:27:18.665Z", 
				NumberOfArchives: 0, 
				SizeInBytes: 0, 
				VaultARN: "arn:aws:glacier:us-west-2:111122223333:vaults/my-vault", 
				VaultName: "my-vault"
			}
		********************************************************************************************* */
		describeVault = () => new Promise((resolve, reject) => {
			var params = {
				accountId: "-", 
				vaultName: this.vaultName
			};
			glacier.describeVault(params, function(err, data) {
				if (err) {
					reject(err);
				} else {
					resolve(data);
				}
			});
		});

	/* *********************************************************************************************
		uploadArchive(params = {}, callback) ⇒ AWS.Request

		https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#uploadArchive-property

			data = {
			archiveId: "kKB7ymWJVpPSwhGP6ycSOAekp9ZYe_--zM_mw6k76ZFGEIWQX-ybtRDvc2VkPSDtfKmQrj0IRQLSGsNuDp-AJVlu2ccmDSyDUmZwKbwbpAdGATGDiB3hHO0bjbGehXTcApVud_wyDw", 
			checksum: "969fb39823836d81f0cc028195fcdbcbbe76cdde932d4646fa7de5f21e18aa67", 
			location: "/0123456789012/vaults/my-vault/archives/kKB7ymWJVpPSwhGP6ycSOAekp9ZYe_--zM_mw6k76ZFGEIWQX-ybtRDvc2VkPSDtfKmQrj0IRQLSGsNuDp-AJVlu2ccmDSyDUmZwKbwbpAdGATGDiB3hHO0bjbGehXTcApVud_wyDw"
			}

		params: obj lit., values {fileSystemId, fileType}
		Uploads the content of a file in unicode format to glacier vault
		Get checksum from buffer and save to archive field
		Returns the archive Id of the file stored in the vault.
	********************************************************************************************* */
	uploadArchive = (key, file, size) => new Promise((resolve, reject) => {
		let vaultName = this.vaultName;
		let body = fs.readFileSync(file);
		let buffer = Buffer.from(body);
		let checksum = glacier.computeChecksums(buffer);
		fs.readFile(file, function(err, buffer) {
			if(err){
				return console.log(err);
			}
			var params = {
				accountId: "-", 				
				archiveDescription: `DocId: ${key}`, // 1,024 printable ASCII characters
				body: body,
				checksum: checksum.treeHash,
				vaultName: vaultName
			};
			glacier.uploadArchive(params, function(err, data) {
				if (err) {
					reject(err);
				} else {
					resolve(data);
				}
			});
		});
	});

	/* *********************************************************************************************
		deleteArchive(params = {}, callback) ⇒ AWS.Request
		Response is 204 No Content if successful

		https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#deleteArchive-property	

	********************************************************************************************* */
	deleteArchive = (params) => new Promise((resolve, reject) => {
		glacier.deleteArchive(params, function(err, data) {
			if (err) {
				reject(err);
			} else {
				// 204 no content
				resolve(data);
			}
		});
	});

	/* *********************************************************************************************
		initiateJob(params = {}, callback) ⇒ AWS.Request

		https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#initiateJob-property

	********************************************************************************************* */
	initiateJob = (params) => new Promise((resolve, reject) => {
		glacier.initiateJob(params, function(err, data) {
			if (err) {
				console.log(err, err.stack);
				reject(err);
			} else {
				// return the jobId
				resolve(data);
			}
		});
	});

	/* *********************************************************************************************
		getJobOutput(params = {}, callback) ⇒ AWS.Request

		https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#getJobOutput-property	

		data = {
			acceptRanges: "bytes", 
			body: <Binary String>, 
			contentType: "application/json", 
			status: 200
		}

		A job ID will not expire for at least 24 hours after Glacier completes the job. 
		For both archive and inventory retrieval jobs, you should verify the downloaded 
		size against the size returned in the headers from the Get Job Output response.
	********************************************************************************************* */
	getJobOutput = (params) => new Promise((resolve, reject) => {
		glacier.getJobOutput(params, function(err, data) {
			if (err) {
				resolve(err);
			} else {
				resolve(data);
			}
		});
	});

	/* *********************************************************************************************
		listJobs(params = {}, callback) ⇒ AWS.Request

		https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#getJobOutput-property	

		data = {
			JobList: [
				{
				Action: "ArchiveRetrieval", 
				ArchiveId: "kKB7ymWJVpPSwhGP6ycSOAekp9ZYe_--zM_mw6k76ZFGEIWQX-ybtRDvc2VkPSDtfKmQrj0IRQLSGsNuDp-AJVlu2ccmDSyDUmZwKbwbpAdGATGDiB3hHO0bjbGehXTcApVud_wyDw", 
				ArchiveSHA256TreeHash: "9628195fcdbcbbe76cdde932d4646fa7de5f219fb39823836d81f0cc0e18aa67", 
				ArchiveSizeInBytes: 3145728, 
				Completed: false, 
				CreationDate: "2015-07-17T21:16:13.840Z", 
				JobDescription: "Retrieve archive on 2015-07-17", 
				JobId: "l7IL5-EkXyEY9Ws95fClzIbk2O5uLYaFdAYOi-azsX_Z8V6NH4yERHzars8wTKYQMX6nBDI9cMNHzyZJO59-8N9aHWav", 
				RetrievalByteRange: "0-3145727", 
				SHA256TreeHash: "9628195fcdbcbbe76cdde932d4646fa7de5f219fb39823836d81f0cc0e18aa67", 
				SNSTopic: "arn:aws:sns:us-west-2:0123456789012:my-vault", 
				StatusCode: "InProgress", 
				VaultARN: "arn:aws:glacier:us-west-2:0123456789012:vaults/my-vault"
			}, 
				{
				Action: "InventoryRetrieval", 
				Completed: false, 
				CreationDate: "2015-07-17T20:23:41.616Z", 
				InventoryRetrievalParameters: {
				Format: "JSON"
				}, 
				JobId: "zbxcm3Z_3z5UkoroF7SuZKrxgGoDc3RloGduS7Eg-RO47Yc6FxsdGBgf_Q2DK5Ejh18CnTS5XW4_XqlNHS61dsO4CnMW", 
				StatusCode: "InProgress", 
				VaultARN: "arn:aws:glacier:us-west-2:0123456789012:vaults/my-vault"
			}
			]
		}
	********************************************************************************************* */
	listJobs = (params) => new Promise((resolve, reject) => {
		glacier.listJobs(params, function(err, data) {
			if (err) {
				reject(err);
			} else {			
				resolve(data);
			}
		});	
	});

	/* *********************************************************************************************
		computeChecksums
		
		https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Glacier.html#computeChecksums-property

	********************************************************************************************* */
	computeChecksums = (data) => new Promise((resolve, reject) => {
		resolve(glacier.computeChecksums(data));	
	});

} // end class

module.exports = PaoGlacier;