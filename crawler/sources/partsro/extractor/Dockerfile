FROM public.ecr.aws/lambda/python:3.11

# install deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy code
COPY . ${LAMBDA_TASK_ROOT}

# lambda handler
CMD ["handler.handler"]
