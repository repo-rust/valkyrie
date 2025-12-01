process_pid=$(ps aux | grep './target/release/valkyrie' | grep -v grep | awk '{print $2}')

for t in $(ls /proc/$process_pid/task); do
    echo "TID $t => $(taskset -p $t 2>/dev/null)"
done
