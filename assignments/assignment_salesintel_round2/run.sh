#!/usr/bin/env bash


# Exit on error. Append "|| true" if you expect an error.
set -o errexit
# Exit On Error Inside Any Functions Or Subshells.
set -o errtrace
# Do Not Allow Use Of Undefined Vars. Use ${VAR:-} To Use An Undefined VAR
set -o nounset
# Catch The Error In Case Mysqldump Fails (But Gzip Succeeds) In `mysqldump |gzip`
set -o pipefail
# Turn On Traces, Useful While Debugging But Commented Out By Default
# set -o xtrace


# MUST BE RUN USING BASH SHELL ONLY
__script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "SCRIPT DIR: ${__script_dir}"


INPUT_ROOT_DIR="${__script_dir}/data/input"
OUTPUT_DIR="${__script_dir}/data/output"
JAR_FILE_PATH="${__script_dir}/target/scala-2.12/assignment_salesintel_round2_2.12-0.1.jar"

echo "INPUT ROOT DIR: ${INPUT_ROOT_DIR}"
echo "OUTPUT DIR: ${OUTPUT_DIR}"
echo "JAR FILE PATH: ${JAR_FILE_PATH}"

echo "Clearing the OUTPUT_DIR before the run(To avoid issues during rerun) using cmd: rm -fr ${OUTPUT_DIR}"
rm -fr "${OUTPUT_DIR}"


# RUN 1
########################################################
echo -e "\nExecuting below command for RUN 1: "

echo "spark-submit \\
  --class assignment.AttributeSelector \\
  --master local[8] \\
  ${JAR_FILE_PATH} \\
 \"${INPUT_ROOT_DIR}/run_1\" \\
 \"${OUTPUT_DIR}\" \\
 495 \\
 None \\
 true"

spark-submit \
  --class assignment.AttributeSelector \
  --master local[8] \
  ${JAR_FILE_PATH} \
  "${INPUT_ROOT_DIR}/run_1" \
  "${OUTPUT_DIR}" \
  495 \
  None \
  true
########################################################


# Intentional sleep for analyzing logs
# Can be commented if not needed
echo -e "\n\nINTENTIONAL SLEEP OF 10 SECONDS BEFORE RUN_2 TO ANALYZE LAST RUN LOGS\n\n"
sleep 10


# RUN 2
########################################################
echo -e "\nExecuting below command for RUN 2: "

echo "spark-submit \\
  --class assignment.AttributeSelector \\
  --master local[8] \\
  ${JAR_FILE_PATH} \\
 \"${INPUT_ROOT_DIR}/run_2\" \\
 \"${OUTPUT_DIR}\" \\
 597 \\
 495 \\
 false"

spark-submit \
  --class assignment.AttributeSelector \
  --master local[8] \
  ${JAR_FILE_PATH} \
  "${INPUT_ROOT_DIR}/run_2" \
  "${OUTPUT_DIR}" \
  597 \
  495 \
  false
########################################################


# Intentional sleep for analyzing logs
# Can be commented if not needed
echo -e "\n\nINTENTIONAL SLEEP OF 10 SECONDS BEFORE RUN_3 TO ANALYZE LAST RUN LOGS\n\n"
sleep 10


# RUN 3
########################################################
echo -e "\nExecuting below command for RUN 3: "

echo "spark-submit \\
  --class assignment.AttributeSelector \\
  --master local[8] \\
  ${JAR_FILE_PATH} \\
 \"${INPUT_ROOT_DIR}/run_3\" \\
 \"${OUTPUT_DIR}\" \\
 648 \\
 495 \\
 true"

spark-submit \
  --class assignment.AttributeSelector \
  --master local[8] \
  ${JAR_FILE_PATH} \
  "${INPUT_ROOT_DIR}/run_3" \
  "${OUTPUT_DIR}" \
  648 \
  495 \
  true
########################################################
