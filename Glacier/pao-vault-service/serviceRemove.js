/**
 * Gregory Bologna
 * Feb 2020
 * https://www.npmjs.com/package/os-service
 */

process.chdir(__dirname);

const service = require("os-service");

service.remove ("glaciervault", function(error) {
	if (error)
		console.trace(error.toString());
});

