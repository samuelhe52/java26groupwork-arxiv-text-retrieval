.PHONY: build run test clean

MVNW := ./mvnw

build:
	$(MVNW) -q -DskipTests package

run:
	$(MVNW) -q spring-boot:run

test:
	$(MVNW) -q test

clean:
	$(MVNW) -q clean
