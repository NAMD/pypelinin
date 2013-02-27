#!/bin/bash

SCRIPT_PATH=$(dirname $(readlink -f $0)) # get parent directory

if [ -z "$1" ] || [ "$1" != "0.1.0" -a "$1" != "fixed" ]; then
    echo "You must specify which pypelinin version to run ('0.1.0' or 'fixed')"
    echo "Usage: $0 <0.1.0|fixed>"
    exit 1
fi

VENV_PATH="$SCRIPT_PATH/venv-pypelinin-$1"
source $VENV_PATH/bin/activate

echo "### Running inside $(basename $VENV_PATH)..."
echo

echo "Starting router..."
python "$SCRIPT_PATH/my_router.py" &
ROUTER_PID=$!
echo "Router has PID $ROUTER_PID"

echo "Starting pipeliner..."
python "$SCRIPT_PATH/my_pipeliner.py" &
PIPELINER_PID=$!
echo "Pipeliner has PID $PIPELINER_PID"

echo "Starting broker..."
python "$SCRIPT_PATH/my_broker.py" &
BROKER_PID=$!
echo "Broker has PID $BROKER_PID"

trap "kill 0; exit" SIGINT SIGTERM SIGKILL

while :
do
    sleep 1
done
