#!/bin/bash

# The arguments that are passed by Jenkins system
TEST_ARG=""
JDK_VERSION="8"
TASK_NAME=""
LOG_LEVEL=""
BRANCH="default"
JEREPO=""
JEREVISION=0
JEREVISIONARG=""


# Arguments for "ant -Dtestcase=*** standalone"
TESTNAME=""
TESTARG1=""
XMLRESULT1=""
LOGRESULT1=""

# Arguments for "ant -Dtestcase=*** -Dargs=*** standalone"
TESTNAMEWITHARG2=""
TESTARG2=""
XMLRESULT2=""
LOGRESULT2=""

# Some standalone test want to test two kinds of different arguments
# Arguments for "ant -Dtestcase=*** -Dargs=*** standalone"
TESTNAMEWITHARG3=""
TESTARG3=""
XMLRESULT3=""
LOGRESULT3=""

# Jenkins VM and Test VM
JENKINSVMIP="slc04ark"
JENKINSVMUSERNAME="jenkins"
JENKINSVM="${JENKINSVMUSERNAME}@${JENKINSVMIP}"
TESTVM=`hostname -s`
TESTVMUSERNAME="tests"
TESTVMUSERPASSWORD="123456"

# The user name used to get the je repository
JEREPUSER="adqian"

# Some basic direcotory/path/filename
BASEDIR="/scratch/tests"
JENKINSBASEDIR="/scratch/jenkins/jobs"
JENKINSBINDIR="/scratch/jenkins/bin"
JESTANDALONEPATH="je/build/test/standalone"
#JERESULTARCH="je_standalone_test_result_archive"
CHANGESETFILE="jenkins_changeset.txt"
ENVINFOFILE="location_of_environment_and_log.txt"

# The script to do error extract
ERROREXTRACTSCRIPT="error_extract_je.sh"
GENXMLSCRIPT="gen_xml.sh"

# Some standalone tests may need different error pattern
# For example, for je_standalone_envsharedcache, its normal
# result output contains "Fail"
ERRORPATTERN="warn exception error fail"
IGNORECASE="true"
error_message="Error: This standalone test fail because it exits with a non-zero exit code"
EXCEPTION_EXPRS="setting je.lock.oldLockExceptions to true"

while getopts "O:j:t:R:b:r:l:T:h:" OPTION
do
	case $OPTION in
		O)
			TEST_ARG=$OPTARG
			;;
		j)
			JDK_VERSION=$OPTARG
			;;
		t)
			TASK_NAME=$OPTARG
			;;
		R)
			JEREPO=$OPTARG
			;;
		b)
			BRANCH=$OPTARG
			;;
		r)
			JEREVISION=$OPTARG
			;;
		l)
			LOG_LEVEL=$OPTARG
			;;
		T)
			TEST_TIMO=$OPTARG
			;;
                h)
                        HGPATH=$OPTARG
                        ;;
	esac
done

if [ "${JEREPO}" == "" ]; then
    echo "JE repository must be specified"
    exit 1
fi

if [ "${JEREVISION}" != "0" ]; then
    JEREVISIONARG=" -u ${JEREVISION}"
fi

if [ "${HGPATH}" != "" ]; then
    HGPATH="${HGPATH}/"
fi

echo "Task name: $TASK_NAME"
echo "Test args: $TEST_ARG"
echo "JE repo: ssh://${JEREPUSER}@${JEREPO}"
echo "JE branch: $BRANCH"
echo "JE revision(0 means the top): $JEREVISION"


## create the dir to save the standalone test result
#cd ${BASEDIR}
#mkdir -p ${JERESULTARCH}
#cd ${JERESULTARCH}
#if [ -d ${TASK_NAME} ]; then
#    rm -rf ${TASK_NAME}
#fi
#mkdir -p ${TASK_NAME}
#cd ${TASK_NAME}
#TEMPDIRNAME=$(date +%Y%m%d%H%M%S)
#mkdir -p ${TEMPDIRNAME}
#SAVEPATH=${BASEDIR}/${JERESULTARCH}/${TASK_NAME}/${TEMPDIRNAME}


# hg clone je
rm -rf ${BASEDIR}/${TASK_NAME} && mkdir -p ${BASEDIR}/${TASK_NAME}
echo "hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO}"
cd ${BASEDIR}/${TASK_NAME} && ${HGPATH}hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO} ./je
cd je && ${HGPATH}hg log -l 1 -v > ./${CHANGESETFILE} && cd ..

# Choose the jdk version
if [ X$JDK_VERSION == X"8" ] ; then
	export JAVA_HOME=${BASEDIR}/app/Java_8
elif [ X$JDK_VERSION == X"7" ] ; then
	export JAVA_HOME=${BASEDIR}/app/Java_7
elif [ X$JDK_VERSION == X"5" ] ; then
	export JAVA_HOME=${BASEDIR}/app/Java_5
elif [ X$JDK_VERSION == X"AIX" ] ; then
        export JAVA_HOME=${BASEDIR}/app/ibm-java-ppc64-80
else
	export JAVA_HOME=${BASEDIR}/app/Java_6
fi

export ANT_HOME=${BASEDIR}/app/ant
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$PATH

ROOT_DIR=${BASEDIR}/${TASK_NAME}
TEST_DIR=${ROOT_DIR}/${JESTANDALONEPATH}

ANT_VERN=`ant -version`
BUILD_VER=`cd $ROOT_DIR/je && ${HGPATH}hg parent`

echo " "
echo "========================================================="
echo "                                                        "
java -version
ant -version
echo "JAVA_HOME=$JAVA_HOME                                   "
echo "ANT_HOME=$ANT_HOME                                     "
echo "Code branch: $BRANCH $BUILD_VER                        "
echo "                                                        "
echo "========================================================="
echo " "

if [ X$LOG_LEVEL == X"INFO" ] ; then
	echo "com.sleepycat.je.util.ConsoleHandler.level=INFO" > ${ROOT_DIR}/je/logging.properties
fi

# 1.je_standalone_cleanwsc
if [ "${TASK_NAME}" == "je_standalone_cleanwsc" ]; then
	TESTNAME="CleanWithSmallCache"
	XMLRESULT1="test.xml"
        LOGRESULT1="cleanwsc_log.txt"

	if [ X$TEST_TIMO != X"" ] ; then
		bash je_cwsc_timo $TEST_TIMO
	fi

# 2.je_standalone_closedbevi
elif [ "${TASK_NAME}" == "je_standalone_closedbevi" ]; then
	TESTNAME="ClosedDbEviction"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="closeddbevi_log.txt"

	TESTNAMEWITHARG2="ClosedDbEvictionRecovery"
	TESTARG2="-recovery 10000000"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="closeddbevi_warg_log.txt"

	ERRORPATTERN="FAIL exception Exception error Error"
	IGNORECASE="false"

# 3.je_standalone_envsharedcache
elif [ "${TASK_NAME}" == "je_standalone_envsharedcache" ]; then
	TESTNAME="EnvSharedCache"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="envsharedcache_log.txt"

	TESTNAMEWITHARG2="EnvSharedCacheOpenTest"
	TESTARG2="-opentest"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="envsharedcache_open_log.txt"

	TESTNAMEWITHARG3="EnvSharedCacheEvenTest"
	TESTARG3="-eventest"
	XMLRESULT3="test_3.xml"
	LOGRESULT3="envsharedcache_event_log.txt"

	ERRORPATTERN="Warn warn exception Exception error Error"
	IGNORECASE="false"

# 4. je_standalone_failoverhybrid
elif [ "${TASK_NAME}" == "je_standalone_failoverhybrid" ]; then
	TESTNAME="FailoverHybrid"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="failoverhybrid_log.txt"

	TESTNAMEWITHARG2="EFailoverHybridRepGroup"
	TESTARG2="-repGroupSize 8"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="failoverhybrid_repgroup_log.txt"

# 5.je_standalone_failovermaster
elif [ "${TASK_NAME}" == "je_standalone_failovermaster" ]; then
	TESTNAME="FailoverMaster"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="failovermaster_log.txt"

	TESTNAMEWITHARG2="FailoverMasterRepGroup"
	TESTARG2="-repGroupSize 8"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="failovermaster_repgroup_log.txt"

# 6. je_standalone_failoverrep
elif [ "${TASK_NAME}" == "je_standalone_failoverrep" ]; then
	TESTNAME="FailoverReplica"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="failoverrep_log.txt"

	TESTNAMEWITHARG2="FailoverReplicaRepGroup"
	TESTARG2="-repGroupSize 8"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="failoverrep_repgroup_log.txt"

# 7.je_standalone_ioerror
elif [ "${TASK_NAME}" == "je_standalone_ioerror" ]; then
	TESTNAME="IOErrorStress"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="ioerror_log.txt"

	TESTNAMEWITHARG2="IOErrorStressWithArgs"
	TESTARG2="-cacheMB 1"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="ioerror_cache_log.txt"

	ERRORPATTERN="Warn warn Fail fail \*\*\*Unexpected"
	IGNORECASE="false"
	error_message="Fail: This standalone test fail because it exits with a non-zero exit code"

# 8.je_standalone_memstress
elif [ "${TASK_NAME}" == "je_standalone_memstress" ]; then
	TESTNAME="MemoryStress"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="memstress_log.txt"

	TESTNAMEWITHARG2="MemoryStressDup"
	TESTARG2="-dups"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="memstress_dups_log.txt"

# 9.je_standalone_openenv
elif [ "${TASK_NAME}" == "je_standalone_openenv" ]; then
	TESTNAME="OpenEnvStress"
	XMLRESULT1="test.xml"
        LOGRESULT1="openenv_log.txt"

# 10.je_standalone_remdb
elif [ "${TASK_NAME}" == "je_standalone_remdb" ]; then
	TESTNAME="RemoveDbStress"
	XMLRESULT1="test.xml"
        LOGRESULT1="remdb_log.txt"

# 11.je_standalone_repclean
elif [ "${TASK_NAME}" == "je_standalone_repclean" ]; then
	TESTNAME="ReplicationCleaning"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="repclean_log.txt"

	TESTNAMEWITHARG2="ReplicationCleaningRepNodeNum"
	TESTARG2="-repNodeNum 8"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="repclean_repnodenum_log.txt"

# 12.je_standalone_repdbops
elif [ "${TASK_NAME}" == "je_standalone_repdbops" ]; then
	TESTNAME="ReplicaDbOps"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="repdbops_log.txt"

	TESTNAMEWITHARG2="ReplicaDbOpsNThread"
	TESTARG2="-nThreads 4"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="repdbops_nthread_log.txt"

# 13.je_standalone_repread
elif [ "${TASK_NAME}" == "je_standalone_repread" ]; then
	TESTNAME="ReplicaReading"
	XMLRESULT1="test_1.xml"
        LOGRESULT1="repread_log.txt"

	TESTNAMEWITHARG2="ReplicaReadingThread"
	TESTARG2="-nPriThreads 4 -nSecThreads 4 -txnOps 30"
	XMLRESULT2="test_2.xml"
	LOGRESULT2="repread_thread_log.txt"

# 14. Now it is not a Standalone test. So we just ignore it
# 15.je_standalone_tempdb
elif [ "${TASK_NAME}" == "je_standalone_tempdb" ]; then
	TESTNAME="TemporaryDbStress"
	XMLRESULT1="test.xml"
        LOGRESULT1="tempdb_log.txt"

# 16.je_standalone_txinmthd
elif [ "${TASK_NAME}" == "je_standalone_txinmthd" ]; then
	TESTNAME="TxnInMultiThreadsStress"
	XMLRESULT1="test.xml"
        LOGRESULT1="txinmthd_log.txt"

# 17(Added on 2016-02-06).je_standalone_ttl
# ant -Dtestcase=TTLStress standalone
elif [ "${TASK_NAME}" == "je_standalone_ttl" ]; then
	TESTNAME="TTLStress"
	XMLRESULT1="test.xml"
        LOGRESULT1="ttl_log.txt"

# 18(Added on 2017-05-17).je_standalone_disklimit
elif [ "${TASK_NAME}" == "je_standalone_disklimit" ]; then
        TESTNAME="DiskLimitStress"
        TESTARG1="-nodes 1 -minutes 15"
        XMLRESULT1="test_1.xml"
        LOGRESULT1="disklimit_onenode.txt"

        TESTNAMEWITHARG2="DiskLimitStressHA"
        TESTARG2="-nodes 3 -minutes 15"
        XMLRESULT2="test_2.xml"
        LOGRESULT2="disklimit_HA.txt"

        TESTNAMEWITHARG3="DiskLimitStressViolation"
        TESTARG3="-nodes 3 -violations true -minutes 25"
        XMLRESULT3="test_3.xml"
        LOGRESULT3="disklimit_violation.txt"

# Task name can not be empty
elif [ X"${TASK_NAME}" == X"" ]; then
	echo "You must specify the task name"
        exit 1

# The wrong task name
else
	echo "The task name is wrong. Please check"
        exit 1
fi

# Copy the error_extract_je.sh and gen_xml.sh
scp ${JENKINSVM}:${JENKINSBINDIR}/${ERROREXTRACTSCRIPT} ${BASEDIR}/
scp ${JENKINSVM}:${JENKINSBINDIR}/${GENXMLSCRIPT}  ${BASEDIR}/

# Back up the result of this time test run. Determin the store directory.
BUILDID=`ssh -l ${JENKINSVMUSERNAME} ${JENKINSVMIP} "cat ${JENKINSBASEDIR}/${TASK_NAME}/nextBuildNumber"`
BUILDID=`expr $BUILDID - 1`

# Since there are many standalone tests running on one test VM,
# the disk is easy to be exhausted. We just reserve the data
# file(.jdb) of each standalone test until to next build. 
if [ -d ${BASEDIR}/log_archive/${TASK_NAME} ]; then
    rm -rf ${BASEDIR}/log_archive/${TASK_NAME}
fi
SAVEPATH=${BASEDIR}/log_archive/${TASK_NAME}/$BUILDID
mkdir -p $SAVEPATH

# DO the Standalone Test
# $1: Test case name
# $2: The name showed in xml files
# $3: The xml files
# $4: The log files
# $5: The error pattern
# $6: Whether the error pattern ignore cases
# $7: The Arguments for the standalone test
#     It is placed at last because it may be empty
do_standalone_test() {
	date_start=`date +"%s"`
	cd ${ROOT_DIR}/je && ant -Dtestcase="$1" -Dargs="$8" standalone
	retvalue=$?
	if [ "${retvalue}" != 0 ]; then
	   echo ${error_message} >> ${ROOT_DIR}/${JESTANDALONEPATH}/log
	fi
	date_end=`date +"%s"`
	intervel=$[$date_end - $date_start]
	cd ${BASEDIR} && bash ${BASEDIR}/${ERROREXTRACTSCRIPT} ${ROOT_DIR}/${JESTANDALONEPATH}/log output.log "JE.Standalone" "$2" $intervel ${ROOT_DIR}/je/"$3" "${retvalue}" "$5" "$6" "${BASEDIR}" "$7"
	cd ${TEST_DIR} && cp log "$4"
        cp -r ${TEST_DIR} ${SAVEPATH}/standalone_$2
}

# ant -Dtestcase=*** standalone
do_standalone_test "${TESTNAME}" "${TESTNAME}" "${XMLRESULT1}" "${LOGRESULT1}" "${ERRORPATTERN}" "${IGNORECASE}" "${EXCEPTION_EXPRS}" "${TESTARG1}"

# ant -Dtestcase=*** -Dargs=*** standalone
if [ X"${TESTNAMEWITHARG2}" != X"" ]; then
	do_standalone_test "${TESTNAME}" "${TESTNAMEWITHARG2}" "${XMLRESULT2}" "${LOGRESULT2}" "${ERRORPATTERN}" "${IGNORECASE}" "${EXCEPTION_EXPRS}" "${TESTARG2}"
fi

# ant -Dtestcase=*** -Dargs=*** standalone
if [ X"${TESTNAMEWITHARG3}" != X"" ]; then
	do_standalone_test "${TESTNAME}" "${TESTNAMEWITHARG3}" "${XMLRESULT3}" "${LOGRESULT3}" "${ERRORPATTERN}" "${IGNORECASE}" "${EXCEPTION_EXPRS}" "${TESTARG3}"
fi


# Generate the test environment information, including log/data store directory
echo "Host: ${TESTVM}.us.oracle.com" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "Username: ${TESTVMUSERNAME}" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "Password: ${TESTVMUSERPASSWORD}" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "Directory for \"ant -Dtestcase=$TESTNAME standalone\" is:" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "    ${SAVEPATH}/standalone_$TESTNAME" >> ${ROOT_DIR}/je/${ENVINFOFILE}
if [ X"${TESTNAMEWITHARG2}" != X"" ]; then
	echo "Directory for \"ant -Dtestcase=$TESTNAME -Dargs='${TESTARG2}' standalone\" is:" >> ${ROOT_DIR}/je/${ENVINFOFILE}
	echo "    ${SAVEPATH}/standalone_$TESTNAMEWITHARG2" >> ${ROOT_DIR}/je/${ENVINFOFILE}
fi
if [ X"${TESTNAMEWITHARG3}" != X"" ]; then
	echo "Directory for \"ant -Dtestcase=$TESTNAME -Dargs='${TESTARG3}' standalone\" is:" >> ${ROOT_DIR}/je/${ENVINFOFILE}
	echo "    ${SAVEPATH}/standalone_$TESTNAMEWITHARG3" >> ${ROOT_DIR}/je/${ENVINFOFILE}
fi


ssh -l ${JENKINSVMUSERNAME} ${JENKINSVMIP} "rm -rf ${JENKINSBASEDIR}/${TASK_NAME}/workspace/*"
# Copy the needed files to jenkins VM
cd ${ROOT_DIR}/je && scp ./${CHANGESETFILE} ./${ENVINFOFILE} ./${XMLRESULT1} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
cd ${TEST_DIR} && scp ./${LOGRESULT1} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/

if [ X"${TESTNAMEWITHARG2}" != X"" ]; then
	cd ${ROOT_DIR}/je && scp ./${XMLRESULT2} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
	cd ${TEST_DIR} && scp ./${LOGRESULT2} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
fi

if [ X"${TESTNAMEWITHARG3}" != X"" ]; then
	cd ${ROOT_DIR}/je && scp ./${XMLRESULT3} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
	cd ${TEST_DIR} && scp ./${LOGRESULT3} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
fi

# Since we save the result of this time run to ${SAVEPATH}/standalone_*,
# we delete the files here to save some disk space.
if [ -d ${TEST_DIR} ]; then
    rm -rf ${TEST_DIR}
fi
