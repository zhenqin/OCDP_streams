#!/bin/bash

proc_id=`cat pid.log`
if [[ -z $proc_id ]];then
    echo "The server is not running ! "
else
     echo "------stop the server!------"
     for id in ${proc_id[*]}
     do
       #echo ${id}
       #thread=`ps -mp ${id}|wc -l`
       #echo "threads number: "${thread}
       kill ${id}

       if [ $? -eq 0 ];then

            echo "stop the server success ! "
            echo > pid.log
       else
            echo "stop the server failed ! "
       fi
     done
fi
