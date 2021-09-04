// file handling
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const appendFile = promisify(fs.appendFileSync);

class PaoRestorer {
	
	constructor(restorePath, fileSizeBytes, fileBuffer) {
		this.restorePath = restorePath;
		this.fileSizeBytes = fileSizeBytes;
		this.fileBuffer = fileBuffer;
	}

	/* *********************************************************************************************
		restoreFile
		Handles the restore process
	********************************************************************************************* */
	restoreFile = () => new Promise((resolve, reject) => {

		try {
		//	fs.writeFileSync(this.restorePath, this.fileBuffer, {encoding: 'utf8'});
			fs.writeFileSync(this.restorePath, this.fileBuffer, { encoding: null });
			resolve(true);
		} catch (error) {
			reject(error);
		}
		
		// fs.writeSync(fd, buffer[, offset[, length[, position]]])#
		// History
		// fd <integer>
		// buffer <Buffer> | <TypedArray> | <DataView>
		// offset <integer>
		// length <integer>
		// position <integer>
		// Returns: <number> The number of bytes written.

	});

} // end class

module.exports = PaoRestorer;