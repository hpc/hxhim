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
thallium_init
thallium_engine
thallium_addrs
Put
flush
process_fill
process_shuffle
Shuffled
Hash
Bulked
ProcessBulk
Pack
Destruct
Transport
Unpack
Cleanup_RPC
Result
collect_stats
remote
local
destroy
print
"

#FindDst

for name in ${names}
do
    if [[ "${file}" -nt "${file}.${name}" ]]
    then
        echo "Regenerating ${name}"
        grep -E "[0-9]+ ${name} [0-9]+.*" "${file}" > "${file}.${name}" &
    fi
done
wait

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
     '${file}.init_thallium'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 56 title 'init::transport thallium',             \
     '${file}.thallium_init'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 48 title 'thallium init function',               \
     '${file}.thallium_engine'    using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 40 title 'thallium start engine',                \
     '${file}.thallium_addrs'     using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 40 lc rgb "pink" title 'thallium get all addrs', \
     '${file}.Put'                using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'PUT',                                  \
     '${file}.flush'              using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'flush',                                \
     '${file}.process_fill'       using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 72 title 'fill (shuffle + switch + check)',      \
     '${file}.process_shuffle'    using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 64 title 'shuffle (hash + find_dst + bulk)',     \
     '${file}.Hash'               using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 48 title 'hash',                                 \
     '${file}.Bulked'             using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 48 title 'move data to bulk',                    \
     '${file}.remote'             using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 72 title 'process remote',                       \
     '${file}.ProcessBulk'        using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 64 title 'Process single bulk packet',           \
     '${file}.Pack'               using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'pack',                                 \
     '${file}.Destruct'           using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'Destruct single remote',               \
     '${file}.Transport'          using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'transport',                            \
     '${file}.Unpack'             using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'unpack',                               \
     '${file}.Cleanup_RPC'        using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'cleanup rpc',                          \
     '${file}.Result'             using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 56 title 'serialize results',                    \
     '${file}.local'              using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 72 title 'process local',                        \
     '${file}.destroy'            using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 80 title 'destroy results',                      \
     '${file}.print'              using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 lc rgb "brown" title 'print',                 \

EOF
wait
