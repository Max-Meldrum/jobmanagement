#!/bin/bash

RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
CYAN=$(tput setaf 6)
BLUE=$(tput setaf 4)
WHITE=$(tput setaf 7)
RESET=$(tput sgr0)

SCALA_VER="2.12"


mkdir -p build

function cpy_jars {
    case "$1" in
    "ye")
        cp executor/yarn/target/scala-"$SCALA_VER"/yarn-executor.jar build/yarn-executor.jar
        ;;
    "sm")
        cp runtime/statemanager/target/scala-"$SCALA_VER"/statemanager.jar build/statemanager.jar
        ;;
    "am")
        cp runtime/appmanager/target/scala-"$SCALA_VER"/appmanager.jar build/appmanager.jar
        ;;
    "all")
        cp runtime/statemanager/target/scala-"$SCALA_VER"/statemanager.jar build/statemanager.jar
        cp runtime/appmanager/target/scala-"$SCALA_VER"/appmanager.jar build/appmanager.jar
        cp cluster-manager/standalone/target/scala-"$SCALA_VER"/standalone.jar build/standalone.jar
        ;;
    esac
}


case "$1" in
    "ye"|"yarnExecutor")
        echo $GREEN"Compiling YarnExecutor..."
        sbt_fail=0
        (sbt yarnExecutor/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile YarnEecutor"
            exit 1
        else
            echo $GREEN"yarn-executor.jar is being moved to build/"
            cpy_jars "ye"
        fi
        ;;
    "am"|"appmanager")
        echo $GREEN"Compiling AppManager..."
        sbt_fail=0
        (sbt appmanager/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile AppManager"
            exit 1
        else
            echo $GREEN"appmanager.jar is being moved to build/"
            cpy_jars "am"
        fi
        ;;
    "statemanager"|"sm")
        echo $BLUE"Compiling StateManager..."
        sbt_fail=0
        (sbt statemanager/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile StateManager"
            exit 1
        else
            echo $BLUE"statemanager.jar is being moved to build/"
            cpy_jars "sm"
        fi
        ;;  
    *)
        echo $WHITE"Compiling everything..."
        sbt_fail=0
        (sbt statemanager/assembly);
        (sbt appmanager/assembly) || sbt_fail=1
        if [[ $sbt_fail -ne 0 ]];
        then
            echo $RED"Failed to compile"
            exit 1
        else
            echo $WHITE"Moving appmanager and statemanager jars to build/"
            cpy_jars "all"
        fi
        ;;
esac


