#!/bin/bash

paras=$#
if [[ "$paras" -ne 4 ]] 
then
	echo "need operation, number of processes, number of maps, number of reduces"
	exit 1
fi

op=$1
if [[ "map" != "$op" ]] && [[ "reduce" != "$op" ]]
then
	echo "the first parameter, operation, needs to be map or reduce. your op is $op"
	exit 1
fi

processes=$2
re='^[0-9]+$'
if ! [[ "$processes" =~ $re ]]
then
	echo "the second parameter, number of processes, needs to be a integer. yours is $processes";
	exit 1
fi

maps=$3
if ! [[ "$maps" =~ $re ]]
then
        echo "the third parameter, number of total maps, needs to be a integer. yours is $maps";
        exit 1
fi

reduces=$4
if ! [[ "$reduces" =~ $re ]]
then
        echo "the fourth parameter, number of total reduces, needs to be a integer. yours is $reduces";
        exit 1
fi

st=0
extent=$maps
if [[ "$op" == "reduce" ]]
then
	extent=$reduces
fi


echo "arguments are: $*"

declare -a hosts=("sr135" "sr136" "sr137")
subst=0

len=${#hosts[@]}
mod=$((extent%len))
if [[ "$mod" -ne 0 ]]
then
	echo "extent, $extent, needs to be a multiple of number of hosts, $len"
	exit 1
fi
subext=$((extent/len))

script_path=$(dirname $(realpath "$0"))
for i in  $(seq 0 $((len-1)))
do
	subst=$((i*subext))
	pdsh -R ssh -w ${hosts[$i]} "$script_path/test_dkey_akey.sh $1 $2 daos_server 24359114-489c-444f-be63-185a553c7d17 bf6a6ebb-85d3-4d89-aeb0-b1942fc91a6c  $maps $reduces 128000 $subst $subext 1024" > ./${hosts[$i]} 2>&1 &
done
