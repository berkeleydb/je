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
REMOTEBASEDIR="/scratch/tests"
JENKINSBASEDIR="/scratch/jenkins/jobs"
CHANGESETFILE="jenkins_changeset.txt"
ENVINFOFILE="location_of_environment_and_log.txt"

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

# 1.Only je_unit_aix needs to be handled specially
if [ "${TASK_NAME}" == "je_unit_aix" ]; then
	JDK_VERSION="AIX"
	BASEDIR="/scratch/nosql"
        TESTVMUSERNAME="nosql"
	TESTVMUSERPASSWORD="q"
fi

echo "Task name: $TASK_NAME"
echo "Test args: $TEST_ARG"
echo "JE repo: ssh://${JEREPUSER}@${JEREPO}"
echo "JE branch: $BRANCH"
echo "JE revision(0 means the top): $JEREVISION"

# hg clone je
if [ "${TASK_NAME}" == "je_unit_aix" ]; then
	ssh tests@slc04arq "rm -rf ${REMOTEBASEDIR}/${TASK_NAME} && mkdir -p ${REMOTEBASEDIR}/${TASK_NAME}"
        echo "hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO}"
	ssh tests@slc04arq "cd ${REMOTEBASEDIR}/${TASK_NAME} && ${HGPATH}hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO} ./je"
	ssh tests@slc04arq "cd ${REMOTEBASEDIR}/${TASK_NAME}/je && ${HGPATH}hg log -l 1 -v > ./jenkins_changeset.txt"
        BUILD_VER=`ssh tests@slc04arq "cd ${REMOTEBASEDIR}/${TASK_NAME}/je && ${HGPATH}hg parent"`
	rm -rf ${BASEDIR}/${TASK_NAME}
	scp -r tests@slc04arq:${REMOTEBASEDIR}/${TASK_NAME} ${BASEDIR}
else
	rm -rf ${BASEDIR}/${TASK_NAME} && mkdir -p ${BASEDIR}/${TASK_NAME}
	echo "hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO}"
	cd ${BASEDIR}/${TASK_NAME} && ${HGPATH}hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO} ./je
	cd je && ${HGPATH}hg log -l 1 -v > ./${CHANGESETFILE} && cd ..
	BUILD_VER=`cd ${BASEDIR}/${TASK_NAME}/je && ${HGPATH}hg parent`
fi

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

if [ X$LOG_LEVEL == X"INFO" ]; then
	echo "com.sleepycat.je.util.ConsoleHandler.level=INFO" > ${ROOT_DIR}/je/logging.properties
fi

if [ X$PRE == X"TRUE" ]; then
	echo " je.rep.preserveRecordVersion=true" >> ${ROOT_DIR}/je/test/je.properties
fi

if [ X$TASK_NAME == X"je_unit_jdk7_no_embedded_ln" ]; then
	echo "je.tree.maxEmbeddedLN=0" >> ${ROOT_DIR}/je/test/je.properties
fi

if [ "${TASK_NAME}" == "je_unit_aix" ]; then
	TEST_ARG="-Dproxy.host=www-proxy -Dproxy.port=80 -Djvm=${JAVA_HOME}/bin/java"
fi

cd ${ROOT_DIR}/je && ant -lib ${BASEDIR}/app/ant/lib/junit-4.10.jar test $TEST_ARG

# Back up the result of this time test run 
#echo "ssh -l ${JENKINSVMUSERNAME} ${JENKINSVMIP} 'cat ${JENKINSBASEDIR}/${TASK_NAME}/nextBuildNumber'"
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
cd ${ROOT_DIR}/je && scp -r build/test/data/ ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
