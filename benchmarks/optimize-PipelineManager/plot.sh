#!/bin/bash

for filename in $(ls test-*_pipelines-pypelinin-fixed.dat); do
    filename=${filename:0:-10}
    sed "s/FILENAME/$filename/g" graphs.gnu | gnuplot
done
