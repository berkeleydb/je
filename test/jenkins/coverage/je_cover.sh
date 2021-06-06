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
CLOVERDIR="clover_for_je"
CHANGESETFILE="jenkins_changeset.txt"
ENVINFOFILE="location_of_environment_and_log.txt"

while getopts "O:j:t:R:b:r:l:h:" OPTION
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

# hg clone je
rm -rf ${BASEDIR}/${TASK_NAME} && mkdir -p ${BASEDIR}/${TASK_NAME}
echo "hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO}"
cd ${BASEDIR}/${TASK_NAME} && ${HGPATH}hg clone -b ${BRANCH} ${JEREVISIONARG} ssh://${JEREPUSER}@${JEREPO} ./je
cd je && ${HGPATH}hg log -l 1 -v > ./${CHANGESETFILE} && cd ..
BUILD_VER=`cd ${BASEDIR}/${TASK_NAME}/je && ${HGPATH}hg parent`

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
export CLASSPATH=${BASEDIR}/app/ant/lib/junit-4.10.jar:$CLASSPATH

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

if [ X$LOG_LEVEL == X"INFO" ] ; then
	echo "com.sleepycat.je.util.ConsoleHandler.level=INFO" > ${ROOT_DIR}/je/logging.properties
fi

# In previous method,we will use a diff file to change the interanl.xml
# and build.xml to be suitable for je cover test. But this method has a
# disadvantage, i.e. every time we change this two files, we need to
# generate the new diff file, other the diff file can not apply for the
# new je version. The diff file generation is very complicate.
#       scp ${JENKINSVM}:~/bin/je_cover.diff ${ROOT_DIR}/je/
#       cd ${ROOT_DIR}/je && ${HGPATH}hg import --no-commit ./je_cover.diff
# In current method, we know which place need to be modified.
# So we directly substitute these places.
scp ${JENKINSVM}:${JENKINSBINDIR}/${CLOVERDIR}/clover*.* ${BASEDIR}/app/ant/lib
cd ${ROOT_DIR}/je
sed -i 's/inheritall=\"false\"/inheritall=\"true\"/g' ./ant/internal.xml
sed -i 's/name=\"clover.tmpdir\" value=\"${builddir}\/tmp\"/name=\"clover.tmpdir\" value=\"${builddir}\/clover_tmp\"/g' ./build.xml
sed -i 's/name=\"clover.libdir\" value=\"\/clover\/lib\"/name=\"clover.libdir\" value=\"\/scratch\/tests\/app\/ant\/lib\"/g' ./build.xml
sed -i 's/inheritall=\"false\"/inheritall=\"true\"/g' ./build.xml
sed -i 's/format=\"frames\"/format=\"noframes\"/g' ./build.xml
sed -i 's/resource=\"clovertasks\"/resource=\"cloverlib.xml\" classpathref=\"clover.classpath\"/g' ./build.xml

ant -lib junit-4.10.jar clean clover.alltestsdone -Dclover.ignorefailure=true


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
cd ${ROOT_DIR}/je && scp ./${CHANGESETFILE} ./${ENVINFOFILE} cloverage.xml ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/
cd ${ROOT_DIR}/je && scp -r clover_html build/test/data/ ${JENKINSVM}:${JENKINSBASEDIR}/${TASK_NAME}/workspace/




#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clean $TEST_ARG1
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar init-clover $TEST_ARG1
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clean-clover $TEST_ARG1
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clover.setup $TEST_ARG1
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clover.runtest 
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clean clover.singletestdone -Dtestcase=com.sleepycat.persist.test.EvolveProxyClassTest 
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clover.alltestsrun $TEST_ARG1
#cd ${ROOT_DIR}/je && ant -lib junit-4.10.jar clover.alltestsdone $TEST_ARG1

# log files
#cd ${ROOT_DIR}/je && tar czf ${TASK_NAME}.tar.gz ./build ./build.xml ./jenkins_changeset.txt ./cloverage.xml ./clover_html 
#cd ${ROOT_DIR}/je && scp ${TASK_NAME}.tar.gz jenkins@slc04ark:~/jobs/${TASK_NAME}/workspace/
