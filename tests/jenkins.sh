#!/bin/sh

# JENKINS_VERBOSE 
#     TRUE : print logs and stdio while running the tests
#     else : print logs and stdio only on failure, after running the test
#
# JENKINS_EXIT_ON_FAIL:
#     TRUE : exit immediately when a test fails
#     else : run all tests, independent of success/failure (exit code reflects
#            failures though)

\trap shutdown QUIT TERM EXIT


export FAILED=0

export TEST_OK="\nJENKINS TEST SUCCESS\n"

export SAGA_VERBOSE=DEBUG
export RADICAL_VERBOSE=DEBUG
export RADICAL_UTILS_VERBOSE=DEBUG
export RADICAL_PILOT_VERBOSE=DEBUG

export HTML_TARGET="../report/test_results.html"
export HTML_SUCCESS="<font color=\"\#66AA66\">SUCCESS</font>"
export HTML_FAILURE="<font color=\"\#AA6666\">FAILED</font>"

# ------------------------------------------------------------------------------
#
html_start()
{
    (
        echo "<html>"
        echo " <body>"
        echo "  <table>"
        echo "   <tr>"
        echo "    <td> <b> Test    </b> </td> "
        echo "    <td> <b> Result  </b> </td> "
        echo "    <td> <b> Logfile </b> </td> "
        echo "  </tr>"
    ) > $HTML_TARGET
}


# ------------------------------------------------------------------------------
#
html_entry()
{
    name=$1
    result=$2
    logfile=$3

    (
        echo "  <tr>"
        echo "   <td> $name    </td> "
        echo "   <td> $result  </td> "
        echo "   <td> <a href=\"$logfile\">log</a> </td> "
        echo " </tr>"
    ) >> $HTML_TARGET
}


# ------------------------------------------------------------------------------
#
html_stop()
{
    (
        echo "  </table>"
        echo " </body>"
        echo "</html>"
    ) >> $HTML_TARGET
}


# ------------------------------------------------------------------------------
#
run_test() {

    name="$1";  shift
    cmd="$*"

    echo "# -----------------------------------------------------"
    echo "# TEST $name: $cmd"
    echo "# "

    log="./rp_test.$name.log"

    if test "$JENKINS_VERBOSE" = "TRUE"
    then
        progress='print'
    else
        progress='printf "."'
    fi

    (set -e ; $cmd ; printf "$TEST_OK") 2>&1 | tee "$log" | awk "{$progress}"

    if grep "$TEST_OK" "$log"
    then
        html_entry "$s ($t)" "$HTML_SUCCESS" "$log"
        echo "# "
        echo "# SUCCESS $s $t"
        echo "# -----------------------------------------------------"
    else
        html_entry "$s ($t)" "$HTML_FAILURE" "$log"
        echo "# "
        echo "# FAILED $s $t"
        echo "# -----------------------------------------------------"

        FAILED=1
    fi


    if test "$JENKINS_EXIT_ON_FAIL" = "TRUE" -a "$FAILED" = "TRUE"
    then
        shutdown
    fi
}


# ------------------------------------------------------------------------------
#
startup()
{
    html_start
}


# ------------------------------------------------------------------------------
#
shutdown()
{
    html_stop
    mv *.log ../report
    exit $FAILED
}


# ------------------------------------------------------------------------------
#
startup

for s in integration mpi
do
    tests=`cat jenkins.cfg | sed -e 's/#.*//g' | grep -v '^ *$'  | grep "$s" | cut -f 1 -d :`
    for t in $tests
    do
        run_test "test_$s_$t" "./test_$s.py $t"
    done
done

issues=`cat jenkins_issues.cfg | sed -e 's/#.*//g' | grep -v '^ *$'`
for i in $issues
do
    run_test "issue_$i" "./$i"
done

shutdown
#
# ------------------------------------------------------------------------------
