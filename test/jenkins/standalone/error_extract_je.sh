rm -f $2

ls $1 >& list.txt

error_pattern="$8"
ignore_case="$9"
base_dir="${10}"
exception_exprs="${11}"

#exception_exprs="setting je.lock.oldLockExceptions to true"

# error_exprs is the possibel error patters, it may looks like
# "warn exception error fail unexpected"
error_exprs="${error_pattern}"
while read LINE
do
	start_pos=0

	# find the first position where an potential error pattern appears
	for fail_message in $error_exprs
	do
		#sed -n "$fail_message" $LINE >& fails.txt
                if [ "${ignore_case}" == "true" ]; then
                	grep -n -i "$fail_message" $LINE | grep -v "$exception_exprs" | cut -d ":" -f 1 > fails.txt
		else
                	grep -n "$fail_message" $LINE | grep -v "$exception_exprs" | cut -d ":" -f 1 > fails.txt
		fi
		err_pos=`sed -n '1p' fails.txt`
		if [ "$err_pos" = "" ]; then
			err_pos=0
		fi

		if [ "$err_pos" != 0 ]; then
			if [ "$start_pos" = 0 ]; then
				start_pos=$err_pos
			elif [ "$start_pos" -gt 0 ]; then
				if [ "$start_pos" -ge "$err_pos" ]; then
					start_pos=$err_pos
				fi
			fi
		fi
		rm -rf fails.txt
	done

        # For all standalone tests, 
	#     if it return 0, then it successes
	#     if it return non-0, then it fails. 
	#         Only under this situation,
	#         we need to report the error and the potential error information
	#         But the location will not be accurate, because some expected
	#         exeptions may contain such error pattern, so we should see the log
	if [ "$7" != 0 ]; then
		# cut the following 500 lines as the err log
		if [ "$start_pos" != 0 ]; then
			exist_failure="true"
			end_pos=`expr $start_pos + 500`
			data=`sed -n "$start_pos"','"$end_pos"'p' $LINE`
			message=`sed -n "$start_pos"','"$start_pos"'p' $LINE`
	                message=${message//\&/&amp;}
	                message=${message//\'/&apos;}
	                message=${message//\"/&quot;}
	                message=${message//\</&lt;}
	                message=${message//\>/&gt;}

			bash ${base_dir}/gen_xml.sh "$3" "$4" "$5" 1 1 "$message" "$data" "$6"
		else
			bash ${base_dir}/gen_xml.sh "$3" "$4" "$5" 1 1 "No concrete error message, see the log" "No concrete error message, see the log" "$6"
		fi
	else
		bash ${base_dir}/gen_xml.sh "$3" "$4" "$5" 1 0 "" "" "$6"
	fi
done < list.txt

# save error log to server
if [ "$exist_failure" = "true" ]; then
	ls *.log >& list.txt
fi


