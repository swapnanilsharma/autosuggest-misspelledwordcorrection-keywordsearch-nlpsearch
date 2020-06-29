# lightweight python
from python:3.7-slim
RUN apt-get update
RUN apt-get install vim -y

# Copy local code into the container image
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install dependencies
RUN pip3 --no-cache-dir install -r requirements.txt

# Run the flask service on container startup
CMD exec gunicorn --bind :2233 --workers 1 --threads 8 app:app