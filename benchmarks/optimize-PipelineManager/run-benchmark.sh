#!/bin/bash

SCRIPT_PATH=$(dirname $(readlink -f $0)) # get parent directory

if [ -z "$1" ] || [ "$1" != "0.1.0" -a "$1" != "fixed" ]; then
    echo "You must specify which pypelinin version to run ('0.1.0' or 'fixed')"
    echo "Usage: $0 <0.1.0|fixed>"
    exit 1
fi

VENV_PATH="$SCRIPT_PATH/venv-pypelinin-$1"
source $VENV_PATH/bin/activate

echo "### Running send_pipelines for $(basename $VENV_PATH)..."
echo

time python "$SCRIPT_PATH/send_pipelines.py" $1
