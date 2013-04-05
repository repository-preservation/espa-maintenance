#/usr/bin/env bash

if [ $# == 2 ]; then
    workDir='./mk_br_tmp';
    mkdir $workDir
    gdal_translate -of GTIFF -sds $1 $workDir/out.tiff;
    gdal_contrast_stretch -ndv -9999 -histeq 100 $workDir/out.tiff3 $workDir/browse.tiff3;
    gdal_contrast_stretch -ndv -9999 -histeq 100 $workDir/out.tiff2 $workDir/browse.tiff2;
    gdal_contrast_stretch -ndv -9999 -histeq 100 $workDir/out.tiff1 $workDir/browse.tiff1;
    gdal_merge_simple -in $workDir/browse.tiff3 -in $workDir/browse.tiff2 -in $workDir/browse.tiff1 -out $2.tif;
    rm -rf $workDir;
else
    echo "make_browse.sh inputfile.hdf outputfile.tif";
fi

