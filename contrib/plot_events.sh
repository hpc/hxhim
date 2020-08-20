#!/usr/bin/env bash

if [[ "$#" -lt 1 ]]
then
    echo "Syntax: $0 file"
    exit 1
fi

file="$1"

# grep -E "[0-9]+ .* [0-9]+.*" "${file}" | grep -v ".*:" > "${file}.filtered"
# awk '{print $2}' "${file}.filtered" | sort | uniq > "${file}.names"

names="
Open
init_leveldb
hxhim_leveldb_open
leveldb_open
init_thallium
Put
flush
fill
shuffle
collect_stats
remote
remote_cleanup
local
destroy
print
"

#FindDst

if [[ "${file}" -nt "${file}.png" ]]
then
    echo "Regenerating individual files"
    for name in ${names}
    do
        grep -E "[0-9]+ ${name} [0-9]+.*" "${file}" > "${file}.${name}" &
    done
    wait
fi

ranks=$(awk '{print $1}' "${file}.Open" | sort | uniq | wc -l)
height=$((${ranks} * 300))

gnuplot <<EOF &

set terminal pngcairo color solid size 16000,${height} font ",64"
set output '${file}.png'
set title "HXHIM Events\nRank R -> Rank R + 1"
set xlabel "Seconds Since Arbitrary Epoch"
set ylabel "Rank"
set clip two
set key outside right
set xrange [0:]
set yrange [0:${ranks}]
set ytics 1

plot '${file}.Open'               using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'hxhim::Open',                          \
     '${file}.init_leveldb'       using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 56 title 'init::datastore leveldb',              \
     '${file}.hxhim_leveldb_open' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 48 title 'hxhim datastore leveldb open',         \
     '${file}.leveldb_open'       using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'leveldb::db::open',                    \
     '${file}.init_thallium'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 56 title 'init::transport thallium',            \
     '${file}.Put'                using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'PUT',                                  \
     '${file}.flush'              using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'flush',                                \
     '${file}.fill'               using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 72 title 'fill (shuffle + switch + check)',      \
     '${file}.shuffle'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 64 title 'shuffle (hash + find_dst + bulk)',     \
     '${file}.remote'             using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 72 title 'process remote',                       \
     '${file}.remote_cleanup'     using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 72 title 'remote cleanup',                       \
     '${file}.destroy'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'destroy results',                      \
     '${file}.print'              using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 title 'print',                                \

EOF
wait
