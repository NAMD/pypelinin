reset
set encoding utf8
set terminal pngcairo size 2000,1000

set output 'time.png'
set size 1,1
set ylabel "Duration"
set xlabel "Pipelines"
set format x "%5.0s %c"
set format y "%5.3s %cs"
set key below box
plot "FILENAME-0.1.0.dat" using 2 title "0.1.0" linecolor rgb "#FF0000" with lines, \
     "FILENAME-fixed.dat" using 2 title "fixed" linecolor rgb "#0000FF" with lines

set output 'memory.png'
set size 1,1
set ylabel "Memory Usage (Virtual)"
set y2label "Memory Usage (Resident)"
set xlabel "Pipelines"
set ytics nomirror
set y2tics
set autoscale  y
set autoscale y2
set format y "%5.3s %cB"
set format y2 "%5.3s %cB"
set format x "%5.0s %c"
set key below box
plot "FILENAME-0.1.0.dat" \
       using 3 title "Virtual (0.1.0)" with lines linecolor rgb "#FF0000" axis x1y1, \
    "" using 4 title "Resident (0.1.0)" with lines linecolor rgb "#FFA0A0" axis x2y2, \
    "FILENAME-fixed.dat" \
       using 3 title "Virtual (fixed)" with lines linecolor rgb "#0000FF" axis x1y1, \
    "" using 4 title "Resident (fixed)" with lines linecolor rgb "#A0A0FF" axis x2y2
