
// file handling
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const appendFile = promisify(fs.appendFileSync);

let m_error_count 		= 0;
let m_delete_count 		= 0;
let m_orphan_count 		= 0;
let m_service_count 	= 0;
let m_debug						= false;

class PaoLogger {
	
	constructor(log) {

		const _logDate = new Date();
		const timestamp = Date.now();
		
		const d = [_logDate.getFullYear(), _logDate.getMonth() + 1, _logDate.getDate()];
		let _logFileDate = d.map((d) => d.toString().padStart(2, '0')).join('-');

		// log format options
		// YYYY-mm-dd_HH-MM-SS
		// YYYY-mm-dd
		
		if(log?.justDate === "false") {
			const t = [_logDate.getHours(), _logDate.getMinutes(), _logDate.getSeconds()];
			_logFileDate += '_' + t.map((t) => t.toString().padStart(2, '0')).join('-');
		}
		
		// add class properties

		m_debug = log.isDebug==="true";
		log.fileName = log.fileName + "-debug";

		// used for start\end duration
		this.logStartTime	= new Date();
		this.logFileDateTime = new Date(Math.floor(timestamp));
		this.filePath = path.join(log.filePath, `${log.fileName}-${_logFileDate}.log`);

	}
	
	static get isDebug() { 
		return m_debug; 
	}

	/* *********************************************************************************************
		property, errorCount
			public
		 	return number of errors logged
			usage: PaoLogger.counter++;
			caller: logger.errorCount()
	********************************************************************************************* */
	errorCount = () => {
		return PaoLogger.error_counter;
	};
	static get error_counter() { return m_error_count; }
	static set error_counter(value) { 
		if(!PaoLogger.isDebug) m_error_count=value; 
	}

	/* *********************************************************************************************
		property, deleteCount
			public
		 	return number of deleted documents
			usage: PaoLogger.counter++;
			caller: logger.deleteCount()
	********************************************************************************************* */
	deleteCount = () => {
		return PaoLogger.delete_counter;
	};
	static get delete_counter() { return m_delete_count; }
	static set delete_counter(value) { m_delete_count=value; }

	/* *********************************************************************************************
		property, orphanCount
			public
		 	return number of documents found orphaned and deleted
			usage: PaoLogger.counter++;
			caller: logger.orphanCount()
	********************************************************************************************* */
	orphanCount = () => {
		return PaoLogger.orphan_counter;
	};
	static get orphan_counter() { return m_orphan_count; }
	static set orphan_counter(value) { m_orphan_count=value; }	

	/* *********************************************************************************************
		property, serviceCount
			public
		 	return number of service jobs run
			usage: PaoLogger.counter++;
			caller: logger.serviceCount()
	********************************************************************************************* */
	serviceCount = () => {
		return PaoLogger.service_counter;
	};
	static get service_counter() { return m_service_count; }
	static set service_counter(value) { m_service_count=value; }	

	/* return a timestamp */
	static timeStamp() {
		var d = new Date();
		var t = [d.getFullYear(), d.getMonth() +1, d.getDate()];
		var dts = t.map((t) => t.toString().padStart(2, '0')).join('-');
		t = [d.getHours(), d.getMinutes(), d.getSeconds()];
		dts += '\xa0' + t.map((t) => t.toString().padStart(2, '0')).join(':');
		return dts;
	}

	/* *********************************************************************************************
	 logError
	  params: error, a formatted error string
	********************************************************************************************* */
	logError = (error) => {
		PaoLogger.error_counter++;

		if(PaoLogger.isDebug)
			console.error(error);

		var content = `${PaoLogger.timeStamp()}\xa0${error}`;
		appendFile(this.filePath, `${content}\n`);
	};

	/* *********************************************************************************************
	 logDeleted
	  params: document Id
		02/22/2021, gwb, changed to support json arg
		{docId,	archiveId}
		change sws \xa0 to tab
	********************************************************************************************* */
	logDeleted = (p) => {
		PaoLogger.delete_counter++;

		if(PaoLogger.isDebug)
			console.log(JSON.stringify(p));

		//var content = `${PaoLogger.timeStamp()}\xa0${p.docId}\xa0${p.archiveId}`;
		var content = `${PaoLogger.timeStamp()} docId:${p.docId},archiveId:${p.archiveId},scenario:${p.scenario}`;
		appendFile(this.filePath, `${content}\n`);
	};

	/* *********************************************************************************************
	 logOrphan
	  params: document Id
	********************************************************************************************* */
	logOrphan = (p) => {
		PaoLogger.orphan_counter++;

		if(PaoLogger.isDebug)
		console.log(JSON.stringify(p));

		// var content = `${PaoLogger.timeStamp()}\xa0${msg}`;
		var content = `${PaoLogger.timeStamp()} docId:${p.docId},archiveId:${p.archiveId},scenario:${p.scenario}`;
		appendFile(this.filePath, `${content}\n`);
	};

	/* *********************************************************************************************
	 logService
	  params: a formatted message string
	********************************************************************************************* */
	logService = (msg) => {
		PaoLogger.service_counter++;

		if(PaoLogger.isDebug)
			console.log(msg);

		var content = `${PaoLogger.timeStamp()}\xa0${msg}`;
		appendFile(this.filePath, `${content}\n`);
	};

}
module.exports = PaoLogger;