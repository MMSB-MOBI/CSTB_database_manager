#!/bin/bash

function usage(){
    echo "This script will download new reference and representative complete genomes from latest refseq version in output directory, and write metadata in output metadata. 
    
    usage : download_refseq.sh <output metadata> <output directory>"
}

if [[ $# -ne 2 ]]; then 
    usage
fi

METADATA=$1
OUTDIR=$2

module load ncbi-genome-download

echo "== Check and download reference genomes..."
#ncbi-genome-download-runner.py -F fasta -l complete -R reference -o $OUTDIR --flat-output -p 12 -r 50 -m $METADATA.reference bacteria -v
echo "== Check and download representative genomes..."
#ncbi-genome-download-runner.py -F fasta -l complete -R representative -o $OUTDIR --flat-output -p 12 -r 50 -m $METADATA.representative bacteria -v

echo "Concatenate reference and representative into $METADATA"
echo -e "#fasta\ttaxid\tname\tgcf\taccession\tftp" > $METADATA
tail -n +2 $METADATA.reference | awk -F "\t" '{print $23 "\t" $8 "\t" $10 "\t" $1 "\t" $22}' >> $METADATA
tail -n +2 $METADATA.representative | awk -F "\t" '{print $23 "\t" $8 "\t" $10 "\t" $1 "\t" $22}' >> $METADATA




