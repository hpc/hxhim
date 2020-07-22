#!/usr/bin/env bash

if [[ "$#" -lt 1 ]]
then
    echo "Syntax: $0 file"
    exit 1
fi

file="$1"

names="
generate
put
barrier
flush_put
Cached
fill
Shuffled
Hash
FindDst
Bulked
Pack
remote
local
Transport
Unpack
Result
destroy
cleanup
"

# for name in ${names}
# do
#     grep -E "[0-9]+ ${name} [0-9]+.*" "${file}" > "${file}.${name}" &
# done
# wait

ranks=$(awk '{print $1}' "${file}.barrier" | sort | uniq | wc -l)

gnuplot <<EOF &

set terminal pngcairo color solid size 12000,2000 font ",32"
set output '${file}.png'
set title "User Requests"
set xlabel "Seconds Since Arbitrary Epoch"
set ylabel "Rank"
set clip two
set key outside right
set yrange [0:${ranks}]
set ytics 1

plot '${file}.put'       using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'put',       \
     '${file}.barrier'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'barrier',   \
     '${file}.flush_put' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'flush_put', \
     '${file}.fill'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 title 'fill',      \
     '${file}.Hash'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth  8 title 'hash',      \
     '${file}.Bulked'    using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth  8 title 'bulk',      \
     '${file}.remote'    using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 title 'remote',    \
     '${file}.Pack'      using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth  8 title 'pack',      \
     '${file}.Transport' using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth  8 title 'transport', \
     '${file}.Unpack'    using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth  8 title 'unpack',    \
     '${file}.local'     using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 16 title 'local',     \
     '${file}.Result'    using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth  8 title 'result',    \
     '${file}.destroy'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'destroy',   \
     '${file}.cleanup'   using (\$3/1e9):1:(\$4-\$3)/1e9:(0) with vectors nohead filled linewidth 32 title 'cleanup',   \

EOF
wait
