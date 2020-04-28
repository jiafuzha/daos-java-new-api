#!/bin/bash
#set -x
if [[ "$#" -ne 11 ]]
then
	echo "needs 11 arguments"
	exit 1
fi

op=$1
processes=$2
shift 2
maps=$4
reduces=$5
part_size=$6
st=$7
extent=$8
seed=$9

echo "===arguments are $op $processes $*"
mod=$((extent%processes))
if [[ "$mod" -ne 0 ]]
then
	echo "extent, $extent, needs to be a multiple of processes, $processes"
	exit 1
fi

subext=$((extent/processes))
echo "sub extent is $subext"

echo "LIB PATH: $LD_LIBRARY_PATH"
script_path=$(dirname $(realpath "$0"))
if ! [[ -d "$script_path/mr_output" ]]
then
	mkdir -p "$script_path/mr_output"
fi

for i in $(seq 0 $((processes-1)))
do
	subst=$(((i*subext)+st))
	echo "sub start $i is $subst"
	$script_path/test_map_reduce.o "$op" "$1" "$2" "$3" "$4" "$5" "$6" "$subst" "$subext" "$9" > $script_path/mr_output/$i 2>&1 &
	pids[$i]=$!
done

for pid in ${pids[*]}; do
	wait $pid
done
total=0
for i in $(seq 0 $((processes-1)))
do
	line=$(grep ":perf:" $script_path/mr_output/$i)
	echo "$i: $line"
	if [[ -z $line ]]
	then
		echo "error in process $i"
		continue
	fi
	v=$(echo $line | sed 's/[^0-9.]*//g')
	total=$(echo $total + $v | bc)
done

echo "=======================>:perf: $total"
