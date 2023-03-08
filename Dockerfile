FROM python:3.10-slim

# set/create app dir
WORKDIR /estools

# add for install
ADD ./requirements.txt requirements.txt

# install package dependencies
RUN pip install -r ./requirements.txt

# add code
ADD ./src/main.py main.py
ADD ./src/estools estools

# run
ENTRYPOINT ["python", "main.py"]
