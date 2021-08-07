FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
RUN mkdir -p ${WORKDIR}/modules
WORKDIR ${WORKDIR}
COPY spec/python_command_spec.json ${WORKDIR}/python_command_spec.json
COPY modules ${WORKDIR}/modules


ENV DATAFLOW_PYTHON_COMMAND_SPEC ${WORKDIR}/python_command_spec.json

RUN pip install avro-python3 pyarrow==0.15.1 apache-beam[gcp]==2.27.0  pandas-datareader==0.9.0
RUN apt-get update && apt-get install -y iputils-ping dnsutils 

COPY __init__.py ${WORKDIR}/__init__.py
COPY setup.py ${WORKDIR}/setup.py
COPY main.py ${WORKDIR}/main.py


# Super important to add these lines.
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

RUN echo '----- listing workdir'
RUN ls -la ${WORKDIR}
RUN echo '-- pinging google--'
RUN echo '-- pinging sec.gov'

RUN echo '--- bulding image---'

ENV PORT 8080
ENV HOST 0.0.0.0
EXPOSE 8080


#ENTRYPOINT [ "/opt/apache/beam/boot" ]

