#!/bin/bash

cd /home/oliveirmic/Projetos-dev/BIREME-GIT/csv2mongo/ || exit

export SBT_OPTS="-Xms12g -Xmx18g -XX:+UseG1GC"

sbt "runMain Main $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12}"
ret="$?"

cd - || exit

exit $ret