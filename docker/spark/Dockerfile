FROM apache/spark:3.5.8-python3@sha256:1916d1d43a6ff0e4d7ac682c84c0412e4a609fca11dd56cfdae592dcde3a378c
USER root
WORKDIR /opt/deel
COPY docker/spark/ivysettings.xml /opt/deel/ivysettings.xml
COPY scripts/warmup.py /opt/deel/scripts/warmup.py
RUN mkdir -p /opt/deel/ivy /lake /events \
    && /opt/spark/bin/spark-submit --master 'local[1]' \
       --conf spark.jars.ivy=/opt/deel/ivy \
       --conf spark.jars.ivySettings=/opt/deel/ivysettings.xml \
       --packages io.delta:delta-spark_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
       /opt/deel/scripts/warmup.py \
    && cp /opt/deel/ivy/jars/*.jar /opt/spark/jars/ \
    && chown -R 185:0 /opt/deel /lake /events
COPY --chown=185:0 app /opt/deel/app
COPY --chown=185:0 tests /opt/deel/tests
COPY --chown=185:0 scripts /opt/deel/scripts
ENV PYTHONPATH=/opt/deel
USER 185
ENTRYPOINT ["/bin/bash", "/opt/deel/scripts/entrypoint.sh"]
CMD ["pipeline"]
