#!/bin/bash
echo NAME: MANISH GIRI
#echo IP Address: `hostname -I`
#echo IP Address: `awk 'END{print $1}' /etc/hosts`
# echo IP Address: `curl ifconfig.co`
echo IP Address: $(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
echo -----------------------
input='/home/data/'
words=0
max_file_name=''
max_word_count=0
echo Here are your text files and their word counts:
for f in `ls ${input}*.txt`
  do 
    word=`wc -w ${f}| awk '{print $1}'`
    # echo $word
    words=$(($words+$word))
    #words+=word
    # get largest file name and count
    if [ $word -gt $max_word_count ]
    then
      max_file_name=`basename "$f"`
      max_word_count=$word 
      #echo Hey that\'s a large number.
    #pwd
    fi
    echo `basename "$f"`: `wc -w ${f} | awk '{print $1}'`
  done 

echo Total word count: $words
echo File with largest number of words: $max_file_name
echo Last usage: $(date "+%Y-%m-%d  %H:%M:%S")