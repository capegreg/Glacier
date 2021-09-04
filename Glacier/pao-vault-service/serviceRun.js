/***
	Glacier Vault Service
	Created by: GBologna
	Created On: 2/10/2020
	Usage: run from windows service
	2020-07-01, GBologna. change to run jobs using default process.execArgv
 */

process.chdir(__dirname);

// logging class
const PaoLogger = require('../pao-vault-app/PaoLogger');

const service = require("os-service");
const { fork } = require('child_process');
var CronJob = require('cron').CronJob; // added 4/5/2020 by gwb for scheduling by crontab
var fork_path = 'C:\\Glacier\\pao-vault-app';

/*
 cronjob started in delay. use standard cron syntax 
 but with 6 asterisks see here https://www.npmjs.com/package/cron

 https://github.com/kelektiv/node-cron/blob/master/examples/multiple_jobs.js
 
*/

// Note: stop cronJob in callback to prevent overlap for long jobs
// restart cronJob in job exit

/* *********************************************************************************************
	cron_upload_job: every 15 minutes
********************************************************************************************* */
const cron_upload_job = new CronJob('0 */15 * * * *', function() {
	cron_upload_job.stop(); 
	doUploads();
}, null, true, 'America/New_York');

/* *********************************************************************************************
	cron_inventory_job: every 6 hours
********************************************************************************************* */
var cron_inventory_job = new CronJob('0 0 */6 * * *', function() {
	cron_inventory_job.stop(); 
	//doInventory();
}, null, true, 'America/New_York');

/* *********************************************************************************************
	cron_purge_job: every 45 minutes
********************************************************************************************* */
var cron_purge_job = new CronJob('0 */45 * * * *', function() {
	cron_purge_job.stop(); 
	doPurges();
}, null, true, 'America/New_York');

/* *********************************************************************************************
	cron_delete_job: every 45 minutes
********************************************************************************************* */
var cron_delete_job = new CronJob('0 */45 * * * *', function() {
	cron_delete_job.stop(); 
	doDeletes();
}, null, true, 'America/New_York');

/* *********************************************************************************************
	each function handles the call to the process
	TODO: check child_process instance
********************************************************************************************* */
function doUploads() {
	try {
		// var job = fork(`${fork_path}\\vault-upload.js`, {execArgv:['--inspect']});
		var job = fork(`${fork_path}\\vault-upload.js`);
		job.on('message', (msg) => {
			if(msg !== "0")
				loggerService.logService(`${msg}`);
			console.log('Message from doUploads fork:', msg);
		});
		job.on('exit', code => {
			cron_upload_job.start(); // restart cronJob
			console.log(`\nUploads job completed on ${(new Date()).toISOString()}.\nProcess exit code is: ${code}\n`);
		});
	} catch (error) {
		loggerError.logError(`doUploads() -> ${error}`);
	}
}

/* *********************************************************************************************
	doInventory
********************************************************************************************* */
function doInventory() {
	try {
		// var job = fork(`${fork_path}\\vault-inventory.js`, {execArgv:['--inspect']});
		var job = fork(`${fork_path}\\vault-inventory.js`);
		job.on('message', (msg) => {
			loggerService.logService(`${msg}`);
			console.log('Message from doInventory fork:', msg);
		});
		job.on('exit', code => {
			cron_inventory_job.start(); // restart cronJob
			console.log(`\nInventory job completed on ${(new Date()).toISOString()}.\nProcess exit code is: ${code}\n`);
		});
	} catch (error) {
		loggerError.logError(`doInventory() -> ${error}`);
	}
}

/* *********************************************************************************************
	doPurges
********************************************************************************************* */
function doPurges() {
	try {
		// var job = fork(`${fork_path}\\vault-purge.js`, {execArgv:['--inspect']});
		var job = fork(`${fork_path}\\vault-purge.js`);
		job.on('message', (msg) => {
			if(msg !== "0")
				loggerService.logService(`${msg}`);
			console.log('Message from doPurges fork:', msg);
		});
		job.on('exit', code => {
			cron_purge_job.start(); // restart cronJob
			console.log(`\nPurge job completed on ${(new Date()).toISOString()}.\nProcess exit code is: ${code}\n`);
		});
	} catch (error) {
		loggerError.logError(`doPurges() -> ${error}`);
	}
}

/* *********************************************************************************************
	doDeletes
********************************************************************************************* */
function doDeletes() {
	try {
		// var job = fork(`${fork_path}\\vault-delete.js`, {execArgv:['--inspect']});
		var job = fork(`${fork_path}\\vault-delete.js`);
		job.on('message', (msg) => {
			if(msg !== "0")
				loggerService.logService(`${msg}`);
			console.log('Message from doDeletes fork:', msg);
		});
		job.on('exit', code => {
			cron_delete_job.start(); // restart cronJob
			console.log(`\nDelete job completed on ${(new Date()).toISOString()}.\nProcess exit code is: ${code}\n`);
		});
	} catch (error) {
		loggerError.logError(`doDeletes() -> ${error}`);
	}
}

function delayWork() {
	// not implemented
}

// enable a delay before service actually starts
var delay = (seconds) => {
	//setTimeout(() => delayWork(), seconds*1000);
};

// The callback function will be called when the service receives a stop request
// start immediate after (n) second delay
service.run (delay(1), function () {
	service.stop (0);
	process.exit (-1);
});

// where to write log
let FilePath		= 'logs';
let FileName		= `glacier-service-errors`; // (suffixed to date in logger)
let logProps = {
	filePath: FilePath,
	fileName: FileName,
	justDate: true,
	isDebug: process.env.DEBUG_MODE
}
const loggerError = new PaoLogger(logProps);

// logger for runtime job feedback
FilePath		= 'logs';
FileName		= `glacier-service-jobs`;
logProps = {
	filePath: FilePath,
	fileName: FileName,
	isDebug: false
}
const loggerService = new PaoLogger(logProps);