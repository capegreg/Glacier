/**
 * Gregory Bologna
 * Feb 2020
 * https://www.npmjs.com/package/os-service
 */

process.chdir(__dirname);

const service = require("os-service");

// nodePath: if not specified, C:\Program Files\nodejs\node.exe 
// otherwise, the path to the node binary used to run the service
// specify path or add to same folder as this file
// 
// programPath: The program to run using nodePath, defaults to the value of process.argv[1]
// Ex. Array(2) ["C:\Program Files\nodejs\node.exe", "c:\Glacier\pao-vault-service\serviceRun.js]

var options = {
	displayName: 	"",
	serviceName: 	"",
	programPath: 	"C:\\Glacier\\pao-vault-service\\serviceRun.js",
	nodePath: 		"C:\\Glacier\\pao-vault-service\\node.exe", 
	username: 		"",
	password: 		""
};

service.add (options.serviceName, options, function(error) {
	if (error)
		console.log(error.toString());
});