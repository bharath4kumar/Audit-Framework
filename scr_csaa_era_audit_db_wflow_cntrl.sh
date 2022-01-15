#!/usr/bin/ksh
#
#     SCRIPT-NAME   : scr_csaa_era_audit_db_wflow_cntrl.sh
#
#     DESCRIPTION   : This script manages the insert / update statements on Audit Table T_ADT_WRKFLW_CNTRL_LOG. Based on the value specified to DB_DML_Type flag, this script chooses the respective DML operation on database. For successful execution of the script, all flags and their values should be passed during execution.
#
#     USAGE         : ksh scr_csaa_era_audit_db_wflow_cntrl.sh -w <INFA_Workflow_Name> -o <DB_DML_Type> -s <Batch_Status>
#
#     EXAMPLE       : ksh scr_csaa_era_audit_db_wflow_cntrl.sh -w wf_CSAA_ERA_IDS_AGRMNT -o Insert -s Running
#
#     Flags         : Following list describes the value to be passed for each flag:
#		       -w : Valid and existing ETL workflow name.
#                      -o : The DML Operation type. It can only accept two values i.e. "Insert" / "Update".   
#		       -s : Execution status of ETL workflow. 	
#
#     VARS/ACRONYMS : Following list of variables are used in the script:
#		       V_DML_Type : The type of DML on T_ADT_WRKFLW_CNTRL_LOG table		
#		       V_Batch_Status : The workflow status		
#		       V_Workflow_Name : The workflow name	
#
#     EXIT-CODES    : Following	list defines the exit codes of the script:
#		       131 - No arguments passed to the script	
#		       132 - Improper usage of the script
#	               133 - Failed to insert a record into T_ADT_WRKFLW_CNTRL_LOG table
#	               134 - Failed to update the record in T_ADT_WRKFLW_CNTRL_LOG table
#                      135 - Invalid DML Type passed to script
#
#     NOTES         : Following are the special instructions which need to be noted prior to execution:
#		       1) All the output generated in this script is redirected to the parent script log. If this script is executed separately, all the output will be redirected to user screen.
#		       2) This script uses Oracle wallet for authentication while connecting to database. Hence ensure that Oracle wallet is set-up on the server and required database credentials are entered in wallet prior to script execution.
#                      3)This script uses oracle sqlplus utility for connecting database. Prior to script execution, ensure that ORACLE_HOME & PATH variables are configured correctly in order to make sqlplus accessible to server.
#				
#     AUTHOR        : Bharath Kumar.A
#                       
#
##########################################################################################################################################################
#
#     CHANGE HISTORY:
#
#                       Version-no: 1 - 07-DEC-2010 - Bharath Kumar.A
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

##########################################################################################################################################################
#                                                                                                                                                        #
#                                                                  SCRIPT LOG FILE                                                                       #
#                                                                                                                                                        #
##########################################################################################################################################################

if [ $# -eq 0 ]
then
	print 'No arguments passed to the script.'
	print 'Usage format: ksh scr_csaa_era_audit_db_wflow_cntrl.sh -w <INFA_Workflow_Name> -o <DB_DML_Type> -s <Batch_Status>'
	exit 131
fi

set -- `getopt w:o:s: $*`
if [ $? -ne 0 ]
then
	print 'Improper usage of the script.'
	print 'Usage format: ksh scr_csaa_era_audit_db_wflow_cntrl.sh -w <INFA_Workflow_Name> -o <DB_DML_Type> -s <Batch_Status>'
	exit 132
fi

##########################################################################################################################################################
#                                                                                                                                                        #
#                                                               MAIN BODY OF THE SCRIPT                                                                  #
#                                                                                                                                                        #
##########################################################################################################################################################

while [ $1 != -- ]
do
	case $1 in 
	-o) 
		V_DML_Type=`echo $2 | tr [A-Z] [a-z]`
	;;
	-s) 
		V_Batch_Status="'$2'"
	;;
	-w)
		V_Workflow_Name="'$2'"
	;;	
	esac
shift #---------------- Moves to the next Option
done

case ${V_DML_Type} in
'insert')

	F_LogInfo "Inserting a record into T_ADT_WRKFLW_CNTRL_LOG table for workflow ${V_Workflow_Name}. Insert query executed on Database:"
	sqlplus -s /@AUDIT << ENDSQL
set serveroutput on;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
declare
var_src_sstm_cd T_ADT_DPNDNCY_CNTRL.SRC_SSTM_CD%TYPE;
var_btch_id T_ADT_BTCH_CNTRL_LOG.BTCH_ID%TYPE;
var_btch_stts_cd T_ADT_STTS_CD.STTS_CD%TYPE;
var_sql_stmt VARCHAR2(1000);
begin
select distinct SRC_SSTM_CD into var_src_sstm_cd from T_ADT_DPNDNCY_CNTRL where WRKFLW_NM=${V_Workflow_Name};
select STTS_CD into var_btch_stts_cd from T_ADT_STTS_CD where STTS_CD_DESC=${V_Batch_Status};
select BTCH_ID into var_btch_id from T_ADT_BTCH_CNTRL_LOG where BTCH_NM=var_src_sstm_cd and BTCH_END_TM is null and BTCH_DRTN is null;
var_sql_stmt:='insert into T_ADT_WRKFLW_CNTRL_LOG(BTCH_ID,WRKFLW_STTS_CD,WRKFLW_STRT_TM,WRKFLW_END_TM,WRKFLW_DRTN,WRKFLW_NM) values('||var_btch_id||','||var_btch_stts_cd||',sysdate,null,null,'||chr(39)||${V_Workflow_Name}||chr(39)||')';
dbms_output.put_line(var_sql_stmt);
execute immediate var_sql_stmt;
commit;
end;
/

ENDSQL

	if [ $? -eq 0 ]
	then	
		F_LogInfo "Record inserted into T_ADT_WRKFLW_CNTRL_LOG table for workflow ${V_Workflow_Name}."
	else
		F_LogError "Failed to insert a record into T_ADT_WRKFLW_CNTRL_LOG table for workflow ${V_Workflow_Name}." 133
	fi
;;

'update')

	F_LogInfo "Updating T_ADT_WRKFLW_CNTRL_LOG table for workflow ${V_Workflow_Name}. Update query executed on Database:"
        sqlplus -s /@AUDIT << ENDSQL
set serveroutput on;
WHENEVER SQLERROR EXIT SQL.SQLCODE;
declare
var_wflow_name varchar2(500) := ${V_Workflow_Name};
var_wflow_stts_cd T_ADT_STTS_CD.STTS_CD%TYPE;
var_sql_stmt varchar2(1000);
begin
select STTS_CD into var_wflow_stts_cd from T_ADT_STTS_CD where STTS_CD_DESC=${V_Batch_Status};
var_sql_stmt:='update T_ADT_WRKFLW_CNTRL_LOG set WRKFLW_END_TM=sysdate,WRKFLW_DRTN=round(((sysdate-WRKFLW_STRT_TM)*24*60),1),WRKFLW_STTS_CD='||var_wflow_stts_cd||' where WRKFLW_NM='||chr(39)||var_wflow_name||chr(39)||' and WRKFLW_END_TM is null and WRKFLW_DRTN is null'; 
dbms_output.put_line(var_sql_stmt);
execute immediate var_sql_stmt;
commit;
end;
/
ENDSQL

	if [ $? -eq 0 ]
	then	
		F_LogInfo "Record updated in T_ADT_WRKFLW_CNTRL_LOG table for workflow ${V_Workflow_Name}."
	else
		F_LogError "Failed to update the record in T_ADT_WRKFLW_CNTRL_LOG table for workflow ${V_Workflow_Name}." 134
	fi
;;
*)
	F_LogError "Invalid DML Type passed to script: ${V_DML_Type}." 135
;;
esac
