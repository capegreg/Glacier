########################################
2020-06-09, Gregory Bologna

Parts:

I.  Glacier scheduling
II. Configuring Glacier application environment

########################################

I. Glacier scheduling

The Glacier runtime was written in node.js and managed 
by a scheduler service in Windows Services. The files
for the scheduler service a found here: \\GlacierVault\pao-vault-service

The service includes 5 files and a logger:

1. node.exe 			-- full Node.JS executable to run the node.js applications.
2. package-lock.json	-- list of dependencies.
3. serviceAdd.js		-- used one time to add the Glacier service to Windows services. 
4. serviceRemove.js		-- used one time to remove the Glacier service from Windows services.
5. serviceRun.js		-- all the Glacier jobs to run at specific schedules.
Logger PaoLogger		-- a custom logging app

The Windows Service name is "CAPEGREG Glacier Vault Service"

Do the following to modify the service properties in serviceAdd.js.
If the service is running, first stop and remove it using serviceRemove.js

This is how to execute one of the service files
1. Open a command window as Administrator
2. CD to file location
3. Execute the file

Remove Glacier service:
\\GlacierVault\pao-vault-service>node.exe serviceRemove.js (enter)

Add Glacier service:
\\GlacierVault\pao-vault-service>node.exe serviceAdd.js (enter)

To make changes to the scheduler
1. Stop the Windows Service 
2. Use a text editor to open serviceRun.js

Scheduling jobs

Each cron procedure executes a different job. 
For example, cron_upload_job executes doUploads

Find the cron job you want to schedule.
There are four:

1. cron_upload_job
2. cron_inventory_job
3. cron_purge_job
4. cron_delete_job

Locate the cron range string. 
The cron range includes seconds, whereas Linux crontab does not.

Seconds: 0-59
Minutes: 0-59
Hours: 0-23
Day of Month: 1-31
Months: 0-11 (Jan-Dec)
Day of Week: 0-6 (Sun-Sat)

This would be every 30 minutes
'0 */30 * * * *'

Save file

### Glacier service logging

The log message is provided at the termination of each module.
For example, when the module exits  the job results will be sent
to the Glacier service as a process thread: process.send(msg). The
Glacier service will log messages each time the model exits.

The location of the service logging is set in Glacier service section "where to write log"

FilePath	-- Directory of log file
FileName	-- Name of the log file, which is suffixed to date in logger

Do not start the service until the Glacier runtime environment is configured


II. Configuring Glacier application environment

There are 10 files in the Glacier application environment found here: \\hades\f$\GlacierVault\pao-vault-app

1. 	vault-upload.js		-- handles document uploads
2. 	vault-purge.js		-- handles document purges from AWS
3. 	vault-inventory.js	-- handles AWS inventory processing
4. 	vault-delete.js		-- handles local document deletes
5. 	utils.js			-- system file
6. 	PaoRestorer.js		-- system file
7. 	PaoLogger.js		-- handles all logging
8. 	PaoGlacier.js		-- implements AWS procedures
9. 	glacier.js			-- authenticates AWS
10.	dbconfig.js			-- includes the database connection

Environment settings

Note: The two environments are Test and Production

Locate "IMAGE_ENV" constant. 
The value equals the Doc_Imaging directory "DEV" or "PROD" at this location

\\Doc_Imaging\DEV
\\Doc_Imaging\PROD

\\ Doc_Imaging is now saved to PKG_UTILS package

FUNCTION fnc_get_nas_path RETURN varchar2 is
BEGIN
  return '\\Doc_Imaging\DEV';
END fnc_get_nas_path;


Found in each of these files:

vault-upload.js
vault-purge.js
vault-inventory.js
vault-delete.js

Change AWS region

glacier.js

US East (N. Virginia) us-east-1	-- Production
US East (Ohio) us-east-2		-- Test

Change connectString in dbconfig.js to iaswprod
Make sure to update the AWS credentials in pkg_utils.fnc_get_aws_settings to the correct AWS access keys

Start service



To debug, open launch.json and set the js to run. 







