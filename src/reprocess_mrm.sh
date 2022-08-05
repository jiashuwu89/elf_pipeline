#!/bin/sh

DATA_PRODUCT="mrm"
RELEVANT_PRODUCTS="mrma mrmi"

LOG_DIR="reprocess_${DATA_PRODUCT}_logs_$(date +%s)"
echo "Sending logs to ${LOG_DIR}"
mkdir ${LOG_DIR}

poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --ela -c 2023-01-01 2024-01-01 >> ${LOG_DIR}/ela_${DATA_PRODUCT}_2023.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --elb -c 2023-01-01 2024-01-01 >> ${LOG_DIR}/elb_${DATA_PRODUCT}_2023.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --ela -c 2022-01-01 2023-01-01 >> ${LOG_DIR}/ela_${DATA_PRODUCT}_2022.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --elb -c 2022-01-01 2023-01-01 >> ${LOG_DIR}/elb_${DATA_PRODUCT}_2022.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --ela -c 2021-01-01 2022-01-01 >> ${LOG_DIR}/ela_${DATA_PRODUCT}_2021.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --elb -c 2021-01-01 2022-01-01 >> ${LOG_DIR}/elb_${DATA_PRODUCT}_2021.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --ela -c 2020-01-01 2021-01-01 >> ${LOG_DIR}/ela_${DATA_PRODUCT}_2020.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --elb -c 2020-01-01 2021-01-01 >> ${LOG_DIR}/elb_${DATA_PRODUCT}_2020.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --ela -c 2019-01-01 2020-01-01 >> ${LOG_DIR}/ela_${DATA_PRODUCT}_2019.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --elb -c 2019-01-01 2020-01-01 >> ${LOG_DIR}/elb_${DATA_PRODUCT}_2019.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --ela -c 2018-01-01 2019-01-01 >> ${LOG_DIR}/ela_${DATA_PRODUCT}_2018.log 2>&1
poetry run python run.py -q dump -p ${RELEVANT_PRODUCTS} --elb -c 2018-01-01 2019-01-01 >> ${LOG_DIR}/elb_${DATA_PRODUCT}_2018.log 2>&1
