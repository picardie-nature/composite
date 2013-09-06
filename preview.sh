#!/bin/bash
for f in $(find composite -name "*.tif"); do
        echo -n "$f "
        n=$(gdalinfo $f|grep Overview|wc -l)
        if [[ "$n" == "3" ]]; then
                echo "déjà traité ($n)"
        else
                gdaladdo --config COMPRESS_OVERVIEW JPEG --config PHOTOMETRIC_OVERVIEW YCBCR --config INTERLEAVE_OVERVIEW PIXEL -r gauss $f 2 4 8 16 32 64
        fi
done

