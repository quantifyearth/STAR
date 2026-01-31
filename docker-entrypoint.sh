#!/bin/bash
set -e

# Ensure logs directory exists
mkdir -p "${DATADIR}/logs"

# Change to DATADIR so .snakemake/ metadata is stored there
cd "${DATADIR}"

# Generate timestamped log filename
LOG_FILE="${DATADIR}/logs/snakemake_$(date +%Y%m%d_%H%M%S).log"

echo "Snakemake logs will be written to: ${LOG_FILE}"

# Run snakemake with all passed arguments, capturing output to log file
exec snakemake \
    --snakefile /root/star/workflow/Snakefile \
    --scheduler greedy \
    "$@" \
    2>&1 | tee "${LOG_FILE}"
