

for i in {1..365}; do 
    echo $(gdate -I -d "-$i days") | sed 's/-//g'
done
