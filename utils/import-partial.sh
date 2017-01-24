#!/bin/bash

# import parts. delete succesfully imported ones

runsql () {
   file="$1"
   echo "importing $file: "
   zcat "$file" | mysql filmweb && rm "$file"

}
export -f runsql
echo "files will be removed on success. C-c now or continue with ENTER"
read

ls *.sql.gz |parallel -j 5 runsql {}
