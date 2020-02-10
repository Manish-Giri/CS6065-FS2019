#!/bin/bash
echo NAME: MANISH GIRI
echo IP Address: $(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
echo -----------------------
input='/home/data/'
words=0
serial=1
max_file_name=''
max_word_count=0
echo Here are your text files and their word counts:
for f in `ls ${input}*.txt`
  do 
    word=`wc -w ${f}| awk '{print $1}'`
    words=$(($words+$word))
    if [ $word -gt $max_word_count ]
    then
      max_file_name=`basename "$f"`
      max_word_count=$word 
    fi
    echo $serial.  `basename "$f"`: `wc -w ${f} | awk '{print $1}'`
    serial=$((serial+1))
  done 
echo Total word count: $words
echo File with largest number of words: $max_file_name
echo Last usage: $(date "+%Y-%m-%d  %H:%M:%S")