FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
RUN mkdir -p ${WORKDIR}/modules
WORKDIR ${WORKDIR}

COPY spec/python_command_spec.json ${WORKDIR}/python_command_spec.json
COPY modules ${WORKDIR}/modules

ENV DATAFLOW_PYTHON_COMMAND_SPEC ${WORKDIR}/python_command_spec.json

RUN pip install avro-python3 pyarrow==0.15.1 apache-beam[gcp]==2.27.0

COPY __init__.py ${WORKDIR}/__init__.py
COPY setup.py ${WORKDIR}/setup.py
COPY main.py ${WORKDIR}/main.py



# Super important to add these lines.
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
#RUN python ${WORKDIR}/setup.py install

RUN pwd
RUN echo '----- listing workdir'
RUN ls -la ${WORKDIR}
RUN echo '---- LISTING moduls-----'
RUN ls -la ${WORKDIR}/modules

ENTRYPOINT [ "/opt/apache/beam/boot" ]

