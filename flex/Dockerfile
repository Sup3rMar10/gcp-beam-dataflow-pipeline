FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

COPY payments_beam ./payments_beam
COPY main.py setup.py ./

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
