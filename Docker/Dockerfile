FROM python:3

# Install dependencies
RUN apt-get update && apt-get install -y wget gnupg2

RUN apt-get -y install python3-pip
RUN pip3 install pandas; pip3 install numpy; pip3 install numpy; pip3 install datetime

# Add Confluent repository
RUN wget -qO - https://packages.confluent.io/deb/6.2/archive.key | apt-key add -
RUN echo "deb [arch=amd64] https://packages.confluent.io/deb/6.2 stable main" | tee /etc/apt/sources.list.d/confluent.list

# Install Confluent Kafka
RUN apt-get update && apt-get install -y confluent-community-2.13

#RUN install librdkafka
#RUN apt install python3-dev
RUN pip3 install confluent-kafka
RUN pip3 install scikit-learn
# Expose Kafka ports
EXPOSE 9092 29092

# Start Kafka
CMD ["confluent", "start"]

COPY client.properties client.properties
COPY app.py app.py
ENTRYPOINT python3 app.py
CMD ["python3","app.py"]
