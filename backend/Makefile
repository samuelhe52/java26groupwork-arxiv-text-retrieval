.PHONY: build run run-local run-cluster test clean

MVNW := ./mvnw

build:
	$(MVNW) -q -DskipTests package

run:
	$(MVNW) -q spring-boot:run

run-local:
	$(MVNW) -q spring-boot:run

run-cluster:
	$(MVNW) -q -DskipTests package
	: "$${HADOOP_CONF_DIR:?set HADOOP_CONF_DIR to your Hadoop conf dir}" && HADOOP_CONF_DIR="$$HADOOP_CONF_DIR" APP_HADOOP_JOB_JAR="$(CURDIR)/target/final-assignment-backend-0.0.1-SNAPSHOT.jar.original" $(MVNW) -q spring-boot:run -Dspring-boot.run.profiles=cluster

test:
	$(MVNW) -q test

clean:
	$(MVNW) -q clean
