#!/bin/sh

export RU_RAISE_ON_INIT=RANDOM_10
export RU_RAISE_ON_STOP=RANDOM_15
export RU_RAISE_ON_WATCH=RANDOM_5
export RU_RAISE_ON_WORK=RANDOM_5

export i=0
TIMEOUT=30

while true
do
    i=$((i+1))
    (
        touch test.ok
        (
            sleep $TIMEOUT
            echo 'timed out'
            rm  -f test.ok
            ps -ef | grep merzky | grep -v grep | grep python | cut -c 10-15 | xargs kill
            echo "timeout killed ($testpid)"
        ) &
        watchpid=$!
        python ./component_termination_4.py 2>&1 > test.tmp 2>&1
        kill -9 $watchpid > /dev/null 2>&1

        if test -e test.ok
        then
            printf "%5d OK   `date`\n" $i | tee -a test.log
        else
            printf "%5d FAIL `date`\n" $i | tee -a test.log
        fi
        grep -e '^RuntimeError' test.tmp >> test.log
        rm -f test.tmp
        rm -f test.ok
    )
done

