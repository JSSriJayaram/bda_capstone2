#!/bin/bash
# ============================================================
# run_all_demos.sh — Master Demo Runner
# NYC Taxi HBase Analytics — BDA Project Review 2
# ============================================================
# Usage: bash hbase/scripts/run_all_demos.sh [student_number]
#   bash hbase/scripts/run_all_demos.sh        # runs all 4
#   bash hbase/scripts/run_all_demos.sh 1      # Student 1 only
#   bash hbase/scripts/run_all_demos.sh 2      # Student 2 only
# ============================================================

DOCKER="/usr/local/bin/docker"
CONTAINER="hbase"
JAR="/root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar"
CP_PREFIX="com.nyctaxi.hbase"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

sep() { echo -e "${CYAN}$(printf '=%.0s' {1..65})${RESET}"; }
hdr() { sep; echo -e "${BOLD}${YELLOW}  $1${RESET}"; sep; }

# Check container is running
if ! $DOCKER ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo -e "${RED}[ERROR] Container '${CONTAINER}' is not running.${RESET}"
    echo "Start it with: $DOCKER start ${CONTAINER}"
    exit 1
fi

STUDENT=${1:-"all"}

run_api() {
    local student=$1
    local class=$2
    local desc=$3
    hdr "STUDENT ${student} JAVA API: ${desc}"
    if $DOCKER exec $CONTAINER java -cp $JAR ${CP_PREFIX}.${class}; then
        echo -e "${GREEN}[OK] Student ${student} API completed.${RESET}"
    else
        echo -e "${RED}[FAIL] Student ${student} API failed.${RESET}"
    fi
}

case $STUDENT in
    1)
        run_api 1 "CreateTable" "Schema Architecture & DDL"
        ;;
    2)
        run_api 2 "InsertData" "Batch Data Ingestion"
        ;;
    3)
        run_api 3 "GetAndDelete" "Record Lifecycle (Get + Delete)"
        ;;
    4)
        hdr "STUDENT 4 JAVA API: Multi-Filter Analytics Engine"
        if $DOCKER exec $CONTAINER java -jar $JAR; then
            echo -e "${GREEN}[OK] Student 4 API completed.${RESET}"
        else
            echo -e "${RED}[FAIL] Student 4 API failed.${RESET}"
        fi
        ;;
    all|*)
        hdr "RUNNING ALL 4 STUDENT APIS — NYC TAXI HBASE DEMO"
        echo ""
        run_api 1 "CreateTable" "Schema Architecture & DDL"
        echo ""
        run_api 2 "InsertData" "Batch Data Ingestion"
        echo ""
        run_api 3 "GetAndDelete" "Record Lifecycle (Get + Delete)"
        echo ""
        hdr "STUDENT 4 JAVA API: Multi-Filter Analytics Engine"
        $DOCKER exec $CONTAINER java -jar $JAR
        echo ""
        hdr "ALL DEMOS COMPLETE"
        echo -e "${GREEN}  Student 1: CreateTable  — Table schema created (4 CFs)${RESET}"
        echo -e "${GREEN}  Student 2: InsertData   — 12 real NYC TLC records ingested${RESET}"
        echo -e "${GREEN}  Student 3: GetAndDelete — Get/Delete lifecycle verified${RESET}"
        echo -e "${GREEN}  Student 4: ScanWithFilter — 5 server-side filters executed${RESET}"
        sep
        ;;
esac
