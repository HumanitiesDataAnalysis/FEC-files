f=$1
d=`basename $f | sed 's/.zip//g'`
echo $d
unzip -n -d fec/$d  $f
