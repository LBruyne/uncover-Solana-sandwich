#!/bin/bash

go build .

# ./watcher reset
# nohup ./watcher jito -s 367400000 -t &
nohup ./watcher leader -s 367400000 -t &

# ./watcher sandwich -s 366797880 &
