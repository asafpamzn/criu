CC = gcc
CFLAGS = -Wall -O2 -pthread
LDFLAGS = -lgnutls -lpthread

TARGET = test-tls-conn
SRC = test-tls-conn.c

.PHONY: all clean test certs

all: certs $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

certs:
	@if [ ! -f certs/ca.crt ]; then bash gen-certs.sh; fi

test: all
	./$(TARGET) 8

clean:
	rm -f $(TARGET)
	rm -rf certs
