#!/usr/bin/ksh
#
#     SCRIPT-NAME   : scr_csaa_era_audit_framework.sh
#
#     DESCRIPTION   : This script is a wrapper object and manages the execution of other scripts in Audit Framework. Based on the flags and its values used in calling the script, it chooses the execution type and proceeds further. This script handles following functionalities:
#			1) Check the status of all dependent jobs, decide and proceed on current execution.
#			2) Logging the run details for each workflow,task and every batch in Audit tables.
#			3) Handle any exceptional situations.
#
#     USAGE         : Following are the expected usage formats of this script:
#			ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> 
#			ksh scr_csaa_era_audit_framework.sh -u <INFA_Workflow_Name>
#			ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -t <INFA_Task_Name>
#			ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -r <INFA_Task_Name>
#			ksh scr_csaa_era_audit_framework.sh -s <Batch_Name> -a <Status>
#
#     EXAMPLE       : Following lists the sample executions of this script: 
#		      	ksh scr_csaa_era_audit_framework.sh -w 'wf_CSAA_ERA_IDS_AGRMNT'
#		      	ksh scr_csaa_era_audit_framework.sh -u 'wf_CSAA_ERA_IDS_AGRMNT'	
#		      	ksh scr_csaa_era_audit_framework.sh -w 'wf_CSAA_ERA_IDS_AGRMNT' -t 's_CSAA_ERA_AGRMNT_LND_STG'
#		      	ksh scr_csaa_era_audit_framework.sh -w 'wf_CSAA_ERA_IDS_AGRMNT' -r 's_CSAA_ERA_AGRMNT_LND_STG'
#			ksh scr_csaa_era_audit_framework.sh -s 'HUON' -a 'Running'
#
#     FLAGS	    : Following describes each flag passed to the the script:
#		      	-w : Valid and existing ETL Workflow name that is ready for execution.
#			-u : Valid and existing ETL Workflow name that is completed.
#			-t : Valid and existing ETL Task name that is ready for execution.
#			-r : Valid and existing ETL Task name that is completed.
#			-s : Valid and existing Source system name.
#			-a : Execution status of the batch. It accepts any of the two values i.e. 'Running' / 'Update'
#
#     VARS/ACRONYMS : Following list of variables are used in the script:
#			V_Wflow_Name : The workflow name
#			V_Update_Wflow : The workflow name whose status needs to be updated
#			V_Task_Name : The ETL task name
#			V_Upd_Task_Name : The ETL task name whose status needs to be updated
#			V_Btch_Name : The source system name
#			V_Btch_Status : The current batch status
#			V_Fldr_Nme : The ETL folder name
#
#     EXIT-CODES    : Following list defines the exit codes of the script:	
#			161 - No arguments passed to the script
#			162 - Improper usage of the script
#			163 - Failed to extract the ETL folder name 
#			164 - Failed to execute scr_csaa_era_audit_etl_manager.sh script while executing the pre-functionalities of ETL task
#			165 - Failed to execute scr_csaa_era_audit_db_task_cntrl.sh script while executing the pre-functionalities of ETL task
#			166 - Failed to execute scr_csaa_era_audit_etl_manager.sh script while executing the post-functionalities of ETL task
#			167 - Failed to execute scr_csaa_era_audit_db_task_cntrl.sh script while executing the post-functionalities of ETL task
#			168 - Failed to execute scr_csaa_era_audit_etl_manager.sh script while executing the post-functionalities of ETL workflow
#			169 - Failed to execute scr_csaa_era_audit_db_wflow_cntrl.sh script while executing the post-functionalities of ETL workflow
#			170 - Failed to execute scr_csaa_era_audit_db_wflow_cntrl.sh script while executing the pre-functionalities of ETL workflow
#			171 - Failed to execute scr_csaa_era_audit_db_wflow_cntrl.sh script after dependency check failure in pre-functionalities of ETL workflow
#			172 - Stopped the execution of workflow since dependency check failed
#			173 - Failed to execute scr_csaa_era_audit_etl_manager.sh after successful dependency check in pre-functionalities of ETL workflow
#			174 - Failed to insert a record into Audit table T_ADT_BTCH_CNTRL_LOG
#			175 - Failed to update a record in Audit table T_ADT_BTCH_CNTRL_LOG
#			176 - Invalid batch status passed to the script
#			177 - Incorrect usage of script options
#
#     NOTES         : Following are the special instructions which need to be noted prior to execution: 
#			1) This script uses Oracle wallet for authentication while connecting to database. Hence ensure that Oracle wallet is set-up on the server and required database credentials are entered in wallet prior to script execution.
#			2) This script executes a environment file "env.sh" that contains a list of global variables which are needed for script's successful execution. Ensure that environment file is set to correct location on the server in the script.
#			3) This script uses oracle sqlplus utility for connecting database. Prior to script execution, ensure that ORACLE_HOME & PATH variables are configured correctly in order to make sqlplus accessible to server.
#				
#     AUTHOR        : Bharath Kumar.A
#                       
#
##########################################################################################################################################################
#
#     CHANGE HISTORY:
#
#                       Version-no: 1 - 09-DEC-2010 - Bharath Kumar.A
#			
#
##########################################################################################################################################################


##########################################################################################################################################################
#                                                                                                                                                        #
#                                                              FUNCTIONS USED BY THE SCRIPT                                                              #
#                                                                                                                                                        #
##########################################################################################################################################################


#---------------------- Function which logs the Information Messages (passed as $1) into the Log File

F_LogInfo ()
{
 echo "[`date '+%d-%b-%Y|%H:%M:%S %Z'`]:[INFO]:$*"
}

#---------------------- Function which logs the Warning Messages (passed as $1) into the Log File

F_LogWarn ()
{
 echo "[`date '+%d-%b-%Y|%H:%M:%S %Z'`]:[WARN]:$*"
} 

#---------------------- Function which logs the Error Messages passed as "$1" into the Log File and exits with the Exit Code passed as "$2"

F_LogError ()
{
 echo "[`date '+%d-%b-%Y|%H:%M:%S %Z'`]:[ERR ]:$1. And execution of script completed with Error-Code <$2>"
 exit $2
}

#---------------------- Function which logs the start of Script execution in the scriptlog

F_ScriptStarted ()
{
 echo "\n\n++++++++++++++++++++++++++++++++++++++ Execution of the ${1} ${2} started at "`date`" ++++++++++++++++++++++++++++++++++++++\n\n"
}

#---------------------- Function which logs the end of Script execution in the scriptlog

F_ScriptEnded ()
{
 echo "\n\n+++++++++++++++++++++++++++++++++++++++ Execution of the ${1} ${2} ended at "`date`" +++++++++++++++++++++++++++++++++++++++\n\n"
}

F_GetFolderName()
{
	V_WFLW_NM="'${1}'"
	V_Fldr_Nme=`sqlplus -s /@AUDIT << ENDSQL
set feedback off;
set heading off;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
select distinct FLDR_NM from T_ADT_PRCS_CNTRL where WRKFLW_NM=${V_WFLW_NM};
ENDSQL
`
if [ $? = 0 ]
then
	F_LogInfo "Folder name is: ${V_Fldr_Nme}"
else
	F_LogError "Failed to identify the ETL folder name for workflow ${1}" 163
fi
}

##########################################################################################################################################################
#                                                                                                                                                        #
#                                                                  SCRIPT LOG FILE                                                                       #
#                                                                                                                                                        #
##########################################################################################################################################################

#---------------- Configures the environment variables required for the script.
. /informat/infa_shared_V861/Scripts/ERA/env.sh
V_Log=${V_Script_Log_Dir}/`basename $0`.`date "+%d-%m-%Y"`.log

if [ $# -eq 0 ]
then
	print 'No arguments passed to the script. Usage formats:'
	print 'ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -u <INFA_Workflow_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -t <INFA_Task_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -r <INFA_Task_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -s <Batch_Name> -a <Status>'
	exit 161
fi	     

set -- `getopt w:u:t:r:s:a: $*`
if [ $? -ne 0 ]
then
	print 'Improper usage of the script. Usage formats:'
	print 'ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -u <INFA_Workflow_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -t <INFA_Task_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -r <INFA_Task_Name>'
	print 'ksh scr_csaa_era_audit_framework.sh -s <Batch_Name> -a <Status>'
	exit 162
fi

#---------------- Log file for re-directing script's output and errors.

exec 1>>$V_Log 2>&1

##########################################################################################################################################################
#                                                                                                                                                        #
#                                                               MAIN BODY OF THE SCRIPT                                                                  #
#                                                                                                                                                        #
##########################################################################################################################################################

while [ $1 != -- ]
do
	case $1 in
	-w)
		 V_Wflow_Name=$2
	 ;;
	-u)
		 V_Update_Wflow=$2
	 ;;
	-t)
		 V_Task_Name=$2
	 ;;
	 -r)
		 V_Upd_Task_Name=$2
	 ;;
	 -s) 
	 	 V_Btch_Name="'$2'"
	 ;;
	 -a)
		 V_Btch_Status=`echo $2 | tr [A-Z] [a-z]`
	 ;;
	esac
	
shift   #---------------- Moves to next flag
done

if [ ! -z "${V_Wflow_Name}" -a ! -z "${V_Task_Name}" ]
then
	echo "\n"
	F_LogInfo "Executing the pre-functionality of ETL Task ${V_Task_Name} from workflow ${V_Wflow_Name}"
	F_LogInfo "Identifying the ETL folder" 
	F_GetFolderName "${V_Wflow_Name}"
        . ${V_Script_Dir}/scr_csaa_era_audit_etl_manager.sh -f ${V_Fldr_Nme} -e Get_Status -w ${V_Wflow_Name} -t ${V_Task_Name} || exit 164
	ksh ${V_Script_Dir}/scr_csaa_era_audit_db_task_cntrl.sh -w ${V_Wflow_Name} -t ${V_Task_Name} -o Insert -s ${V_Exec_Status} || exit 165

elif [ ! -z "${V_Wflow_Name}" -a ! -z "${V_Upd_Task_Name}" ]
then
	echo "\n"
        F_LogInfo "Executing the post-functionality of ETL Task ${V_Upd_Task_Name} from workflow ${V_Wflow_Name}"
        F_LogInfo "Identifying the ETL folder"
        F_GetFolderName "${V_Wflow_Name}"
        . ${V_Script_Dir}/scr_csaa_era_audit_etl_manager.sh -f ${V_Fldr_Nme} -e Get_Status -w ${V_Wflow_Name} -t ${V_Upd_Task_Name} || exit 166
        ksh ${V_Script_Dir}/scr_csaa_era_audit_db_task_cntrl.sh -w ${V_Wflow_Name} -t ${V_Upd_Task_Name} -o Update -s ${V_Exec_Status} || exit 167
        
elif [ ! -z "${V_Update_Wflow}" -a -z "${V_Wflow_Name}" -a -z "${V_Task_Name}" -a -z "${V_Upd_Task_Name}" ]
then
	echo "\n"
	F_LogInfo "Executing the post-functionality of ETL Workflow ${V_Update_Wflow}"
	F_LogInfo "Identifying the ETL folder"
	F_GetFolderName "${V_Update_Wflow}"
	. ${V_Script_Dir}/scr_csaa_era_audit_etl_manager.sh -f ${V_Fldr_Nme} -e Get_Status -w "${V_Update_Wflow}" || exit 168
	ksh ${V_Script_Dir}/scr_csaa_era_audit_db_wflow_cntrl.sh -w "${V_Update_Wflow}" -o Update -s "${V_Exec_Status}" || exit 169
	F_ScriptEnded "Workflow" "${V_Update_Wflow}"
        
elif [ ! -z "${V_Wflow_Name}" -a -z "${V_Update_Wflow}" -a -z "${V_Upd_Task_Name}" -a -z "${V_Task_Name}" ]
then
	F_ScriptStarted "Workflow" "${V_Wflow_Name}"
	F_LogInfo "Executing the pre-functionalities of ETL Workflow ${V_Wflow_Name}"
	ksh ${V_Script_Dir}/scr_csaa_era_audit_db_wflow_cntrl.sh -w "${V_Wflow_Name}" -o Insert -s "Running" || exit 170
	F_LogInfo "Checking the status of dependency jobs for workflow ${V_Wflow_Name}"
	V_SQL_Wflow_Name="'${V_Wflow_Name}'"
	sqlplus -s /@AUDIT << ENDSQL > "/tmp/$$.id"
SET SERVEROUTPUT ON;
SET HEADING OFF;
SET FEEDBACK OFF;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
declare
var_btch_id T_ADT_BTCH_CNTRL_LOG.BTCH_ID%TYPE;
var_btch_nme T_ADT_DPNDNCY_CNTRL.SRC_SSTM_CD%TYPE;
var_status number(10);
begin
select distinct SRC_SSTM_CD into var_btch_nme from T_ADT_DPNDNCY_CNTRL where WRKFLW_NM=${V_SQL_Wflow_Name};
select btch_id into var_btch_id from T_ADT_BTCH_CNTRL_LOG where BTCH_NM=var_btch_nme and BTCH_END_TM is null and BTCH_DRTN is null;
for i in (select DPNDNT_WRKFLW_NM from T_ADT_DPNDNCY_CNTRL where WRKFLW_NM=${V_SQL_Wflow_Name} and DPNDNT_WRKFLW_NM is not null)
loop
select WRKFLW_STTS_CD into var_status from T_ADT_WRKFLW_CNTRL_LOG where WRKFLW_NM=i.DPNDNT_WRKFLW_NM and BTCH_ID=var_btch_id and WRKFLW_RUN_ID=(select max(WRKFLW_RUN_ID) from T_ADT_WRKFLW_CNTRL_LOG where WRKFLW_NM=i.DPNDNT_WRKFLW_NM and BTCH_ID=var_btch_id );
if var_status != 10
then
dbms_output.put_line(i.DPNDNT_WRKFLW_NM);
end if;
end loop;
end;
/
ENDSQL
	if [ -s "/tmp/$$.id" ] 
	then
		F_LogInfo "Following dependency jobs were not completed successfully:"
		cat /tmp/$$.id
		ksh ${V_Script_Dir}/scr_csaa_era_audit_db_wflow_cntrl.sh -w "${V_Wflow_Name}" -o Update -s "Stopped" || exit 171
		F_LogError "Stopped the current execution of workflow ${V_Wflow_Name}, since the dependency jobs were not completed successfully" 172
	else
		F_LogInfo "Dependency check completed successfully"
		F_GetFolderName "${V_Wflow_Name}"		
		ksh ${V_Script_Dir}/scr_csaa_era_audit_etl_manager.sh -f "${V_Fldr_Nme}" -e Start_Wflow -w "${V_Wflow_Name}" || exit 173
	fi

elif [ ! -z "${V_Btch_Name}" -a ! -z "${V_Btch_Status}" ]
then
	if [ "${V_Btch_Status}" == "running" ]
	then
		F_ScriptStarted "Batch" "${V_Btch_Name}"
		F_LogInfo "Inserting a record into Audit table T_ADT_BTCH_CNTRL_LOG for batch ${V_Btch_Name}. Insert query executed on Database:"
		sqlplus -s /@AUDIT << ENDSQL
SET SERVEROUTPUT ON;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
declare
var_src_sstm_cd T_ADT_SRC_SSTM.SRC_SSTM_CD%TYPE;
var_sql_stmt varchar2(1000);
begin
select SRC_SSTM_CD into var_src_sstm_cd from T_ADT_SRC_SSTM where src_sstm_cd=${V_Btch_Name};
var_sql_stmt:='insert into T_ADT_BTCH_CNTRL_LOG(BTCH_STRT_TM,BTCH_END_TM,BTCH_DRTN,BTCH_STTS_CD,BTCH_NM) values(sysdate,null,null,6,'||chr(39)||${V_Btch_Name}||chr(39)||')';
dbms_output.put_line(var_sql_stmt);
execute immediate var_sql_stmt;
commit;
end;
/
ENDSQL
		if [ $? -eq 0 ]
		then
			F_LogInfo "Record inserted for batch ${V_Btch_Name} in Audit table T_ADT_BTCH_CNTRL_LOG."
		else
			F_LogError "Failed to insert record for batch ${V_Btch_Name} in Audit table T_ADT_BTCH_CNTRL_LOG." 174
		fi
	elif [ "${V_Btch_Status}" == "update" ]
	then
		echo "\n"
		F_LogInfo "Updating the record in Audit table T_ADT_BTCH_CNTRL_LOG for batch ${V_Btch_Name}. Update query executed on Database:"
		sqlplus -s /@AUDIT << ENDSQL
SET SERVEROUTPUT ON;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
declare
var_btch_id  T_ADT_BTCH_CNTRL_LOG.BTCH_ID%TYPE;
var_count number(10) := 1;
var_sql_stmt varchar2(1000);
begin
select BTCH_ID into var_btch_id from T_ADT_BTCH_CNTRL_LOG where BTCH_NM=${V_Btch_Name} and BTCH_END_TM is null and BTCH_DRTN is null;
select count(1) into var_count from T_ADT_WRKFLW_CNTRL_LOG where BTCH_ID=var_btch_id and WRKFLW_STTS_CD!=10 and wrkflw_nm not in (select wrkflw_nm from T_ADT_WRKFLW_CNTRL_LOG where BTCH_ID=var_btch_id and WRKFLW_STTS_CD=10);
if var_count = 0
then
var_sql_stmt:='update T_ADT_BTCH_CNTRL_LOG set BTCH_END_TM=sysdate,BTCH_DRTN=round(((sysdate-BTCH_STRT_TM)*24*60),1),BTCH_STTS_CD=10 where BTCH_NM='||chr(39)||${V_Btch_Name}||chr(39)||' and BTCH_END_TM is null and BTCH_DRTN is null';
dbms_output.put_line(var_sql_stmt);
execute immediate var_sql_stmt;
commit;
else
var_sql_stmt:='update T_ADT_BTCH_CNTRL_LOG set BTCH_END_TM=sysdate,BTCH_DRTN=round(((sysdate-BTCH_STRT_TM)*24*60),1),BTCH_STTS_CD=8 where BTCH_NM='||chr(39)||${V_Btch_Name}||chr(39)||' and BTCH_END_TM is null and BTCH_DRTN is null';
dbms_output.put_line(var_sql_stmt);
execute immediate var_sql_stmt;
commit;
end if;
end;
/
ENDSQL
		if [ $? -eq 0 ]
		then
			F_LogInfo "Record updated for batch ${V_Btch_Name} in Audit table T_ADT_BTCH_CNTRL_LOG."
		else
			F_LogError "Failed to update the record for batch ${V_Btch_Name} in Audit table T_ADT_BTCH_CNTRL_LOG." 175
		fi
		F_ScriptEnded "Batch" "${V_Btch_Name}"	
	else
		print "Invalid batch status passed to the script. Usage Format:"
		print "ksh scr_csaa_era_audit_framework.sh -s <Batch_Name> -a <Update / Running>"	
		exit 176
	fi
	
else
        F_LogInfo "Incorrect usage of script options."
	print "Usage Format:"
	print "ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name>"        
	print "ksh scr_csaa_era_audit_framework.sh -u <INFA_Workflow_Name>"
	print "ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -t <INFA_Task_Name>"
	print "ksh scr_csaa_era_audit_framework.sh -w <INFA_Workflow_Name> -r <INFA_Task_Name>"
	print "ksh scr_csaa_era_audit_framework.sh -s <Batch_Name> -a <Status>"
	exit 177
fi
