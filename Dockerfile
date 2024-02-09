FROM ubuntu:jammy
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -yq install python3-pip

COPY requirements.txt /app/
RUN pip3 install -r /app/requirements.txt

# copy model 
COPY app/dt_model.joblib /app/

# copy scripts
COPY main.py /app/
COPY constants.py /app/
COPY utils.py /app/
COPY memory_db.py /app/
COPY feed_database.py /app/

RUN chmod +x /app/main.py

COPY messages.mllp /data/
EXPOSE 8440
EXPOSE 8441
CMD /app/main.py --mllp=$MLLP_ADDRESS --pager=$PAGER_ADDRESS
