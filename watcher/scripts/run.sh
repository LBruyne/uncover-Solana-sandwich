#!/bin/bash

go build .

echo "Watcher built successfully."

# Check if any components of watcher is running
# ps -ef | grep watcher
# Kill any specific componets of watcher if needed 
# pkill -f "watcher jito"
# pkill -f "watcher leader" 
# pkill -f "watcher sandwich"

# Reset the db if needed. DO NOT RUN WITHOUT DISCUSSION
# echo "Resetting the database..."
# echo "This will delete all data in the database. Proceed with caution!"
# read -p "Are you sure you want to continue? (yes/no): " confirmation
# if [ "$confirmation" == "yes" ]; then
#     ./watcher reset 
# fi

# Sync only for jito bundles
# -t for disable output to console
# -s for starting slot
# nohup ./watcher jito -s 350000000 -t --disable-task2 &
# sleep 5
# nohup ./watcher jito -s 355000000 -t --disable-task2 &
# sleep 5
nohup ./watcher jito -s 360000000 -t --disable-task2 &

# Sync and scan for sandwich_txs in bundles 
# nohup ./watcher jito -s 367000000 -t &

# Sync slot_leaders
# nohup ./watcher leader -s 367400000 -t &

# Sync blocks and search for sandwiches
# ./watcher sandwich -s 366797880 &
