#!/bin/bash
for i in {5..10}
do
    echo "-----------" >> benchmark_for_tests.txt
    echo "starting runs for thread count of*************************:" >> benchmark_for_tests.txt
    echo $i >> benchmark_for_tests.txt
    echo "*****************-----------************" >> benchmark_for_tests.txt
    for x in {1..10}
    do
    echo "-----------" >> benchmark_for_tests.txt
    echo "try:" >> benchmark_for_tests.txt
    echo $x >> benchmark_for_tests.txt
        cargo nextest run --test-threads=$i --failure-output final --status-level none --final-status-level retry --hide-progress-bar &>> benchmark_for_tests.txt
    echo "-----------" >> benchmark_for_tests.txt
    done
    echo "-----------" >> benchmark_for_tests.txt
    echo "ending runs for thread count of:" >> benchmark_for_tests.txt
    echo $i >> benchmark_for_tests.txt
    echo "-----------" >> benchmark_for_tests.txt
done
