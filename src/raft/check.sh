function Test {
    command=$1
    echo Start Test Case $command ...
    go test -run $command
    if [ $? -ne 0 ]; then
        echo Wa on $command
        exit
    fi
}

for (( i = 1; i <= 50; ++i )) do
    echo --------------------------------------------------------------------------------------
    echo Case: $i
    #Test Election
    #Test BasicAgree
    #Test FailAgree
    #Test FailNoAgree
    #Test ConcurrentStarts
    #Test Rejoin
    Test Backup
done
