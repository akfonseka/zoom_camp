FROM prefecthq/prefect:2.7.7-python3.9

COPY week_2_workflow_orchestration/docker-requirements.txt  .

RUN apt-get update && apt-get install -y curl
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir 

COPY week_2_workflow_orchestration/flows /opt/prefect/flows
COPY ./week_2_workflow_orchestration/ /opt/prefect/home/arith/data-engineering-zoomcamp/week_2_workflow_orchestration
