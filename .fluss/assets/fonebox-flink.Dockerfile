FROM fluss/quickstart-flink:1.20-0.7.0

RUN rm -rf /opt/flink/lib/fluss-flink-1.20-0.7.0.jar
COPY --chown=flink:flink fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-0.7-SNAPSHOT.jar /opt/flink/lib
RUN rm -rf /opt/sql-client/lib/fluss-flink-1.20-0.7.0.jar
COPY --chown=flink:flink fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-0.7-SNAPSHOT.jar /opt/sql-client/lib

RUN ["chmod", "+x", "/opt/sql-client/sql-client"]
