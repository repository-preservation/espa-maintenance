#!/usr/bin/env bash

getSensor() {
#example="LE70290302002125EDC00"
#sanity check $1 to make sure it's a scene and present, etc
    sensor=`echo $1|cut -c 1-3`
    if [ "$sensor" = "LT5" ];
    then
        echo 'tm'
    elif [ "$sensor" = "LE7" ];
    then
        echo 'etm'
    fi
    
}

getPath() {
    p=`echo $1|cut -c 4-6`
    p=${p/#0/}
    p=${p/#0/}
    echo $p
}

getRow() {
    r=`echo $1|cut -c 7-9`
    r=${r/#0/}
    r=${r/#0/}
    echo $r
}

getYear() {
    echo $scene |cut -c 10-13
}

getDoy() {
    echo $scene |cut -c 14-16
}

getStation() {
    echo $scene |cut -c 17-21
}


if [ -z "${ESPA_WORK_DIR}" ];
then
    echo '$ESPA_WORK_DIR not set... exiting'
    exit 1
fi
if [ ! -d $ESPA_WORK_DIR ];
then
    echo "$ESPA_WORK_DIR doesn't exist... creating"
    mkdir -p $ESPA_WORK_DIR
fi


export base_source_path='/data/standard_l1t'
export base_output_path='/data2/LSRD/collections'
export processing_level='sr'
export scene=$1
export path=`getPath $scene`
export row=`getRow $scene`
export sensor=`getSensor $scene`
export year=`getYear $scene`
export doy=`getDoy $scene`
#export source_host='espa@edclpdsftp.cr.usgs.gov'
#export destination_host='espa@edclpsftp.cr.usgs.gov'
export source_host='localhost'
export destination_host='localhost'
export source_file=$base_source_path/$sensor/$path/$row/$year/$scene.tar.gz
export product_filename=$scene-$processing_level
export destination_dir=$base_output_path/$processing_level/$sensor/$path/$row/$year
export destination_file=$destination_dir/$product_filename.tar.gz
export workdir=$ESPA_WORK_DIR/$processing_level/$scene/work
export outputdir=$ESPA_WORK_DIR/$processing_level/$scene/output

if [ -d "$workdir" ];
then
    rm -rf $workdir
fi
mkdir -p $workdir


if [ $? -ne 0 ];
then
    echo "Error cleaning & creating workdir:$workdir... exiting"
    exit 1
fi

if [ -d "$outputdir" ];
then
    rm -rf $outputdir
fi
mkdir -p $outputdir


if [ $? -ne 0 ];
then
    echo "Error cleaning & creating outputdir:$outputdir... exiting"
    exit 2
fi

echo "Transferring $source_file from $source_host to $HOSTNAME"
scp -C $source_host:$source_file $outputdir

if [ $? -ne 0 ];
then
    echo "Error transferring $source_host:$source_file to $outputdir... exiting"
    exit 3;
fi

echo "Unpacking $scene.tar.gz to $workdir"
tar --directory $workdir -xvf $outputdir/$scene.tar.gz

if [ $? -ne 0 ];
then 
    echo "Error unpacking source file to $outputdir/$scene.tar.gz"
    exit 4;
fi

cd $workdir 

echo "Running LEDAPS against $scene"
do_ledaps.py --metafile *_MTL.txt

if [ $? -ne 0 ];
then
    echo "LEDAPS error detected... exiting"
    exit 5;
fi

echo "Purging unneeded files from $workdir"
rm -rf *TIF *VER* README* LogReport*

if [ $? -ne 0 ];
then
    echo "Error purging files from $workdir... exiting"
    exit 6;
fi

echo "Packaging completed product to $outputdir/$product_filename.tar.gz"
tar --directory $workdir -cvf $outputdir/$product_filename.tar *

if [ $? -ne 0 ];
then
    echo "Error packaging finished product to $outputdir/$product_filename.tar"
    exit 7;
fi

gzip $outputdir/$product_filename.tar

if [ $? -ne 0 ];
then
    echo "Error compressing final product file:$outputdir/$product_filename.tar"
    exit 8;
fi

echo "Creating destination directories at $destination_dir"
ssh $destination_host mkdir -p $destination_dir

if [ $? -ne 0 ];
then
    echo "Error creating destination directory $destination_dir on $destination_host"
    exit 9;
fi

echo "Transferring $product_filename.tar to $destination_host:$destination_file"
scp -C $outputdir/$product_filename.tar.gz $destination_host:$destination_file

if [ $? -ne 0 ];
then
    echo "Error transferring $product_filename.tar to $destination_host:$destination_file... exiting"
    exit 10;
fi

cd /tmp

echo "Cleaning local directories:$outputdir $workdir"
rm -rf $outputdir $workdir

if [ $? -ne 0 ];
then
    echo "Error cleaning output[$outputdir] and work[$workdir] directories... exiting"
    exit 11;
fi

echo "Surface Reflectance Complete"
