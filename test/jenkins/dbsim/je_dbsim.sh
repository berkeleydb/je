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
HGPATH=""
PRE=""

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
CHANGESETFILE="jenkins_changeset.txt"
ENVINFOFILE="location_of_environment_and_log.txt"

# The script to do error extract
ERROREXTRACTSCRIPT="error_extract_je.sh"
GENXMLSCRIPT="gen_xml.sh"

# Some standalone tests may need different error pattern
# For example, for je_standalone_envsharedcache, its normal
# result output contains "Fail"
ERRORPATTERN="exception error fail"
IGNORECASE="true"
EXCEPTION_EXPRS="setting je.lock.oldLockExceptions to true"

# DBsim test related
run_class=com.sleepycat.util.sim.Dbsim
CASE=""
VERIFYITER=""

while getopts "O:j:t:R:b:r:l:h:p:" OPTION
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
		h)
			HGPATH=$OPTARG
			;;
		p)
			PRE=$OPTARG
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

# hg clone the repository
rm -rf ${BASEDIR}/${TASK_NAME} && mkdir -p ${BASEDIR}/${TASK_NAME}
echo "hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO}"
cd ${BASEDIR}/${TASK_NAME} && ${HGPATH}hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO} ./je
sleep 3
${HGPATH}hg clone -b default ssh://${JEREPUSER}@sleepycat-scm.us.oracle.com://a/hgroot/dbsim
cd je && ${HGPATH}hg log -l 1 -v > ./${CHANGESETFILE} && cd ..
BUILD_VER=`cd ${BASEDIR}/${TASK_NAME}/je && ${HGPATH}hg parent`

# Check the jdk version
if [ X$JDK_VERSION == X"8" ] ; then
	export JAVA_HOME=${BASEDIR}/app/Java_8
elif [ X$JDK_VERSION == X"7" ] ; then
	export JAVA_HOME=${BASEDIR}/app/Java_7
elif [ X$JDK_VERSION == X"6" ] ; then
	export JAVA_HOME=${BASEDIR}/app/Java_6
elif [ X$JDK_VERSION == X"AIX" ] ; then
        export JAVA_HOME=${BASEDIR}/app/ibm-java-ppc64-80
else
	export JAVA_HOME=${BASEDIR}/app/Java_5
fi

export ANT_HOME=${BASEDIR}/app/ant
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$PATH

ROOT_DIR=${BASEDIR}/${TASK_NAME}
DBSIM_DIR=${ROOT_DIR}/dbsim
DBSIM_CLSDIR=${DBSIM_DIR}/build/classes
DBSIM_TEST_DIR=${DBSIM_DIR}/build/test
ANT_VERN=`ant -version`

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

case ${TASK_NAME} in
	je_dbsim_abortstress)
		CASE="abortstress"
                scp ${JENKINSVM}:${JENKINSBINDIR}/je.${CASE}.conf ${ROOT_DIR}/dbsim/configs/${CASE}.conf
		;;
	je_dbsim_duplicate)
		CASE="duplicate"
		VERIFYITER=100
		;;
	je_dbsim_dwstress)
		CASE="dwStress"
		;;
	je_dbsim_embedded_abort)
		CASE="embedded_abort"
		EXCEPTION_EXPRS="Lock expired. Locker\|setting je.lock.oldLockExceptions to true"
		;;
	je_dbsim_recovery)
		CASE="recovery"
                scp ${JENKINSVM}:${JENKINSBINDIR}/je.${CASE}.conf ${ROOT_DIR}/dbsim/configs/${CASE}.conf
		;;
	*)
		echo "The task name is wrong. Please check."
		exit 1
esac

if [ X$LOG_LEVEL == X"INFO" ]; then
	echo "com.sleepycat.je.util.ConsoleHandler.level=INFO" > ${ROOT_DIR}/je/logging.properties
fi

# compile and generate the je.jar
cd ${ROOT_DIR}/je && ant jar
cd ${ROOT_DIR}/je/build/lib && cp je.jar ${DBSIM_DIR}/lib/

date_start=`date +"%s"`

export CLASSPATH=$CLASSPATH:${DBSIM_CLSDIR}:${DBSIM_DIR}/lib/antlr.jar:${DBSIM_DIR}/lib/je.jar
cd ${DBSIM_DIR}
# compile dbsim source codes
ant clean compile

# create the environment directory for running dbsim tests
mkdir ${DBSIM_TEST_DIR}

# Copy the error_extract_je.sh and gen_xml.sh
scp ${JENKINSVM}:${JENKINSBINDIR}/${ERROREXTRACTSCRIPT} ${BASEDIR}/
scp ${JENKINSVM}:${JENKINSBINDIR}/${GENXMLSCRIPT}  ${BASEDIR}/

# run Dbsim test
case ${CASE} in 
	"recovery"|"duplicate")
		date -u +'%Y-%m-%d %H:%M:%S %Z'
		java -cp $CLASSPATH -DsetErrorListener=true -ea ${run_class} -h ${DBSIM_TEST_DIR} -c ${DBSIM_DIR}/configs/${CASE}.conf -V ${VERIFYITER} -B >& ${DBSIM_DIR}/Case_${CASE}.tmp
		date -u +'%Y-%m-%d %H:%M:%S %Z'
		;;
	"abortstress"|"dwStress"|"embedded_abort")
		date -u +'%Y-%m-%d %H:%M:%S %Z'
		java -cp $CLASSPATH -DsetErrorListener=true -ea ${run_class} -h ${DBSIM_TEST_DIR} -c ${DBSIM_DIR}/configs/${CASE}.conf -I
		date -u +'%Y-%m-%d %H:%M:%S %Z'
		java -cp $CLASSPATH -DsetErrorListener=true -ea ${run_class} -h ${DBSIM_TEST_DIR} -c ${DBSIM_DIR}/configs/${CASE}.conf >& ${DBSIM_DIR}/Case_${CASE}.tmp
		date -u +'%Y-%m-%d %H:%M:%S %Z'
		;;
esac
date_end=`date +"%s"`
intervel=$[$date_end - $date_start]
cd ${BASEDIR} && bash ${BASEDIR}/${ERROREXTRACTSCRIPT} ${DBSIM_DIR}/Case_${CASE}.tmp output.log "JE.DBSim" "${CASE}" $intervel ${ROOT_DIR}/test.xml "0" "${ERRORPATTERN}" "${IGNORECASE}" "${BASEDIR}" "${EXCEPTION_EXPRS}"
cp ${DBSIM_DIR}/Case_${CASE}.tmp ${DBSIM_DIR}/Case_${CASE}.tmp.bk
tail ${DBSIM_DIR}/Case_${CASE}.tmp -n 100 > ${DBSIM_DIR}/DBSim_${CASE}.log

# Back up the result of this time test run 
BUILDID=`ssh -l ${JENKINSVMUSERNAME} ${JENKINSVMIP} "cat ${JENKINSBASEDIR}/${TASK_NAME}/nextBuildNumber"`
BUILDID=`expr $BUILDID - 1`

LOGLOCATION=${BASEDIR}/log_archive/${TASK_NAME}/$BUILDID
mkdir -p $LOGLOCATION
cd $LOGLOCATION
cp -r ${ROOT_DIR}/je $LOGLOCATION

# Generate the test environment information
echo "Host: ${TESTVM}.us.oracle.com" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "Directory: `pwd`" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "Username: ${TESTVMUSERNAME}" >> ${ROOT_DIR}/je/${ENVINFOFILE}
echo "Password: ${TESTVMUSERPASSWORD}" >> ${ROOT_DIR}/je/${ENVINFOFILE}

ssh -l ${JENKINSVMUSERNAME} ${JENKINSVMIP} "rm -rf ${JENKINSBASEDIR}/${TASK_NAME}/workspace/*"
cd ${ROOT_DIR}/je && scp ./${CHANGESETFILE} ./${ENVINFOFILE} ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
scp ${ROOT_DIR}/test.xml ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
scp ${DBSIM_DIR}/DBSim_${CASE}.log ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/


#COMP_VER=B_je-3_3_x
#COMP_DIR=${ROOT_DIR}/compare
#if [ X"$CASE" = X"fileformat" ] ; then
#	# checkout the 
#	mkdir ${COMP_DIR}
#	cd ${COMP_DIR} && hg clone -b default ssh://adqian@sleepycat-scm.us.oracle.com://a/hgroot/je
#	cd ${COMP_DIR}/je && ant jar
#
#	cd ${DBSIM_DIR}
#	mv lib/je.jar lib/main.jar
#	cp ${COMP_DIR}/je/build/lib/je.jar ./lib/compare.jar
#	ant filefmt -Dtestjar.v1=lib/compare.jar -Dtestjar.v2=lib/main.jar >& ${DBSIM_DIR}/Case_${CASE}-JDK_${JAVA_VERN}.tmp
#fi
