#!/bin/bash
mode=$1
environment=$2
shift 2
extraArgs="$*"
yaml=config/${environment}.yaml
projectname=kafkagenericeventparser
projecttype=stream
cd ../../
case $mode in
"build")
    sbt "$projectname/Docker/publishLocal"
;;
"publish")
    sbt "$projectname/Docker/publish"
;;
*)
    sbt "$projectname/run $mode $projecttype/$projectname/$yaml $extraArgs"
;;
esac
