#!/usr/bin/ksh
#
#     SCRIPT-NAME   : scr_csaa_era_audit_etl_manager.sh
#
#     DESCRIPTION   : This script manages the execution of ETL workflows and gets the execution status detail of a worklow / task. Based on the flag value of Execution_Type, script processing continues.
#
#     USAGE         : Following are the expected usage formats of this script:			
#	               ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> 
# 		       ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> < -t <ETL_Task_Name> >
#
#     EXAMPLE       : Following lists the sample executions of this script:
#	               ksh scr_csaa_era_audit_etl_manager.sh -f '$project_ERA' -e Start_Wflow -w 'wf_CSAA_ERA_IDS_AGRMNT' 
#	               ksh scr_csaa_era_audit_etl_manager.sh -f '$project_ERA' -e Get_Status -w 'wf_CSAA_ERA_IDS_AGRMNT' 
#	               ksh scr_csaa_era_audit_etl_manager.sh -f '$project_ERA' -e Get_Status -w 'wf_CSAA_ERA_IDS_AGRMNT' -t 's_CSAA_ERA_IDS_AGRMNT'
#
#     FLAGS	    : Following list describes the value to be passed for each flag:
#		       -f : Valid and existing ETL Folder Name
#		       -e : Type of Execution. It only accepts two values (i.e. Start_Wflow & Get_Status)  		
#		       -w : Valid and existing ETL Workflow Name
#		       -t : Valid and existing ETL Task Name	
#
#     VARS/ACRONYMS : Following list of variables are used in the script:
#                      V_Exec_Type : Type of script execution. Following values are expected: 'start_wflow' / 'get_status'	
#	               V_Task_Nme : The ETL task name
#                      V_Fldr_Nme : The ETL folder name	
#                      V_Wflow_Nme : The ETL workflow name	
#
#     EXIT-CODES    : Following list defines the exit codes of the script: 	
#		       151 - No arguments passed to the script	
#	               152 - Improper usage of the script
#                      153 - Some of the required environment variable(s) are empty and not set
#                      154 - Script failed to start workflow
#                      155 - Improper usage of the script to start workflow
#	               156 - Failed to get the execution status of ETL task
#                      157 - Failed to get the execution status of the workflow
#                      158 - Improper usage of the script to get task details
#                      159 - Invalid Execution type passed to the script
#
#     NOTES         : Following are the special instructions which need to be noted prior to execution: 
#		       1) All the output generated in this script is redirected to the parent script log. If this script is executed separately, all the output will be redirected to user screen.	
#                      2) This script executes a environment file "env.sh" that contains a list of global variables which are needed for script's successful execution. Ensure that environment file is set to correct location on the server in the script.			
#                      3) This script uses Powercenter provided utility pmcmd for starting workflow / getting status of ETL objects. Prior to script execution, ensure that all INFA & PATH variables are configured correctly in order to make pmcmd utility accessible to server.				
#
#     AUTHOR        : Bharath Kumar.A
#                       
#
##########################################################################################################################################################
#
#     CHANGE HISTORY:
#
#                       Version-no: 1 - 08-DEC-2010 - Bharath Kumar.A
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

#---------------- Configures environment variables needed for the script.

. /informat/infa_shared_V861/Scripts/ERA/env.sh

if [ $# -eq 0 ]
then
	print 'No arguments passed to the script. Usage format: '
	print 'ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> '
	print 'ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> < -t <ETL_Task_Name> >'
	exit 151
fi

set -- `getopt w:f:e:t: $*`
if [ $? -ne 0 ]
then
	print 'Improper usage of the script.'
	print 'Usage format: '
	print 'ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> '
	print 'ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> < -t <ETL_Task_Name> >'
	exit 152
fi

##########################################################################################################################################################
#                                                                                                                                                        #
#                                                               MAIN BODY OF THE SCRIPT                                                                  #
#                                                                                                                                                        #
##########################################################################################################################################################

[ ! -z "${INFA_INT_SRVC_NAME}" ] && [ ! -z "${INFA_DOMAIN_NAME}" ] && [ ! -z "${INFA_USER_NAME}" ] && [ ! -z "${PMPASSWORD}" ] && [ ! -z "${INFA_SECURITY_DOMAIN}" ] && F_LogInfo "All environment variables are set with values" || F_LogError "Identified some of the environment variable(s) are empty and not set." 153	

while [ $1 != -- ]
do
	case $1 in 
	-w) 
		V_Wflow_Nme=$2
	;;
	-f) 
		V_Fldr_Nme=$2
	;;
	-t)
		V_Task_Nme=$2
	;;
	-e)
		V_Exec_Type=`echo $2 | tr [A-Z] [a-z]`
	esac
shift #---------------- Moves to the next Option
done

case ${V_Exec_Type} in
'start_wflow')
	if [ ! -z "${V_Fldr_Nme}" -a ! -z "${V_Wflow_Nme}" ]
	then
		F_LogInfo "Starting the execution of ETL workflow ${V_Wflow_Nme} from folder ${V_Fldr_Nme}"
		pmcmd startworkflow -sv "${INFA_INT_SRVC_NAME}" -d "${INFA_DOMAIN_NAME}" -t "${V_TimeOut}" -uv INFA_USER_NAME -pv PMPASSWORD -usdv INFA_SECURITY_DOMAIN -f "${V_Fldr_Nme}" -wait "${V_Wflow_Nme}"

		if [ $? -eq 0 ]
		then
			F_LogInfo "Execution of workflow ${V_Wflow_Nme} from folder ${V_Fldr_Nme} finished."
			ksh ${V_Script_Dir}/scr_csaa_era_audit_framework.sh -u ${V_Wflow_Nme}
		else
			ksh ${V_Script_Dir}/scr_csaa_era_audit_db_wflow_cntrl.sh -w "${V_Wflow_Nme}" -o Update -s "Failed"
			F_LogError "Failed to start the workflow ${V_Wflow_Nme} from folder ${V_Fldr_Nme}. Please check the pmcmd & script logs for more details." 154
		fi
	else
			print 'Improper usage of the script.'
			print 'Usage format: '
			print 'ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e start_wflow -w <ETL_Workflow_Name> '
			exit 155
	fi
;;
'get_status')
	if [ ! -z "${V_Wflow_Nme}" -a ! -z "${V_Fldr_Nme}" -a ! -z "${V_Task_Nme}" ]
	then
		F_LogInfo "Checking the execution status of ETL Task ${V_Task_Nme} in workflow ${V_Wflow_Nme}"
		V_Exec_Status=`pmcmd gettaskdetails -sv "${INFA_INT_SRVC_NAME}" -d "${INFA_DOMAIN_NAME}" -uv INFA_USER_NAME -pv PMPASSWORD -f "${V_Fldr_Nme}" -w "${V_Wflow_Nme}" "${V_Task_Nme}" | grep 'Task run status' | cut -f2 -d":" | sed -e 's/\[//' -e 's/\]//' | xargs echo`
		
		if [ $? -eq 0 ]
		then
			F_LogInfo "Execution status of task ${V_Task_Nme} in workflow ${V_Wflow_Nme} is : ${V_Exec_Status}"
		else	
			F_LogError "Failed to get the execution status of the task ${V_Task_Nme} in workflow ${V_Wflow_Nme}" 156
		fi
	elif [ ! -z "${V_Wflow_Nme}" -a ! -z "${V_Fldr_Nme}" ]
	then
		F_LogInfo "Checking the execution status of ETL Workflow ${V_Wflow_Nme}"
		V_Exec_Status=`pmcmd getworkflowdetails -sv "${INFA_INT_SRVC_NAME}" -d "${INFA_DOMAIN_NAME}" -uv INFA_USER_NAME -pv PMPASSWORD -f "${V_Fldr_Nme}" "${V_Wflow_Nme}" | grep 'Workflow run status' | cut -f2 -d":" | sed -e 's/\[//' -e 's/\]//' | xargs echo`
		
		if [ $? -eq 0 ]
		then
			F_LogInfo "Execution status of workflow ${V_Wflow_Nme} is: ${V_Exec_Status}"
		else
			F_LogError "Failed to get the execution status of the workflow ${V_Wflow_Nme}" 157
		fi
	else
		print 'Improper usage of the script.'
		print 'Usage Format:'
		print 'ksh scr_csaa_era_audit_etl_manager.sh -f <ETL_Folder_Name> -e <Execution_Type> -w <ETL_Workflow_Name> < -t <ETL_Task_Name> >'
		exit 158
	fi
;;
*)
	F_LogError "Invalid Execution type passed to the script. Please check and retry" 159
;;
esac
