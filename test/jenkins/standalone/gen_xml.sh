# $1 classname
# $2 name
# $3 time
# $4 number of tests
# $5 number of failed tests
# $6 error message
# $7 error data
# $8 out file name

#echo "1=$1"
#echo "2=$2"
#echo "3=$3"
#echo "4=$4"
#echo "5=$5"
#echo "6=$6"
#echo "7=$7"
#echo "8=$8"
rm -f $8

echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >> $8
echo "<testsuite timestamp=\"`date`\" time=\"$3\" tests=\"$4\" name=\"$1\" hostname=\"`hostname`\" failures=\"$5\" errors=\"$5\">" >> $8

echo "<testcase time=\"$3\" name=\"$2\" classname=\"$1\">" >> $8

if [ $5 -eq 1 ]; then
	echo "<error type=\"Exception\" message=\"$6\">" >> $8
	echo "<![CDATA[$7]]>" >> $8
	echo "</error>" >> $8

	echo "<failure type=\"Exception\" message=\"Exception\">" >> $8
	echo "<![CDATA[Exception]]>" >> $8
	echo "</failure>" >> $8
fi
echo "</testcase>" >> $8

echo "<system-out><![CDATA[]]></system-out>" >> $8
echo "<system-err><![CDATA[]]></system-err>" >> $8
 
echo "</testsuite>" >> $8
