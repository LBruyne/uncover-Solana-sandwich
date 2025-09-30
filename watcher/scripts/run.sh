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
# nohup ./watcher jito -s 362900000 -t --disable-task2 &
# sleep 5
# nohup ./watcher jito -s 369600000 -t --disable-task2 &
# sleep 5

# Sync and scan for sandwich_txs in bundles 
# nohup ./watcher jito -s 369600000 -t --disable-task1 &
# sleep 5

# Sync blocks and search for sandwiches
nohup ./watcher sandwich -s 368700000 &

# Sync slot_leaders
# nohup ./watcher leader -s 360000000 -t &
# sleep 5

# Lookup logs 
# head -10 ./logs/watcher_20250918_095118_jito_jito.log
# tail -10 ./logs/watcher_20250918_095118_jito_jito.log
# head -10 ./logs/watcher_20250918_080119_jito_jito.log
# tail -10 ./logs/watcher_20250918_080119_jito_jito.log
# head -10 ./logs/watcher_20250924_073440_jito_jito.log
# tail -10 ./logs/watcher_20250924_073440_jito_jito.log
# head -10 ./logs/watcher_20250924_073420_sandwich_sol.log
# tail -10 ./logs/watcher_20250924_073420_sandwich_sol.log