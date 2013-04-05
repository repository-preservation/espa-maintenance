#! /bin/csh -f

if $#argv != 1 then
    echo "Usage: do_ledaps.csh <Landsat_MTL_file>"
    exit
else
    set meta_file = $argv[1]
    set meta = `echo $meta_file | sed -e 's/.txt//' -e 's/_MTL//' -e 's/.met//'`
endif

# run LEDAPS modules
lndpm $meta_file
lndcal lndcal.$meta.txt
lndcsm lndcsm.$meta.txt
lndsr lndsr.$meta.txt
lndsrbm.ksh lndsr.$meta.txt
