## MySQL dump splitter

Usage: `pbzip2 -c -d | ./mysqlsplit` will split it into tables in `./out` catalog and decorate it with some SQL to make restore go faster.


USE AT YOUR OWN RISK

To import put script  `utils/import-partial.sh` in `./out` dir and run it. **IT WILL REMOVE FILES THAT IMPORTED SUCCESSFULLY** (mysql exited with 0 code), edit it out if you don't want it. It is done so interrupted import can still be continued without problem.
