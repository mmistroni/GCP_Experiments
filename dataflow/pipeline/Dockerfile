FROM gcr.io/dataflow-templates-base/python3-template-launcher-base


RUN mkdir -p /dataflow/template
WORKDIR /dataflow/template

COPY ./* ${WORKDIR}/

COPY spec/python_command_spec.json ${WORKDIR}/python_command_spec.json

ENV DATAFLOW_PYTHON_COMMAND_SPEC ${WORKDIR}/python_command_spec.json

RUN pip install avro-python3 pyarrow==0.15.1 apache-beam[gcp]==2.27.0

COPY setup.py ${WORKDIR}/setup.py
COPY main.py ${WORKDIR}/main.py

# Super important to add these lines.
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"