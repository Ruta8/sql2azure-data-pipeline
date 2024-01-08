FROM openjdk:8-jdk

RUN apt-get update && \
    apt-get install -y python3.9 python3-pip && \
    apt-get clean;

WORKDIR /app

COPY src/ .

# Install necessary Python dependencies and Hadoop Azure
RUN pip3 install pyspark azure-storage-blob

EXPOSE 80

RUN curl -L "https://go.microsoft.com/fwlink/?linkid=2247860" -o sqljdbc_12.4.2.0_enu.tar.gz

RUN mkdir -p /usr/local/share/sqljdbc

RUN tar -xzf sqljdbc_12.4.2.0_enu.tar.gz -C /usr/local/share/sqljdbc && rm sqljdbc_12.4.2.0_enu.tar.gz

CMD ["python3.9", "app.py"]
