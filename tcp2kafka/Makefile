
all: tcp2kafka
CC=gcc
LIBS=-lm -lcurl -lzstd -lssl -lcrypto -lz -ldl -lpthread -lrt
LDFLAGS=-I ./ /usr/local/lib/librdkafka.a

tcp2kafka: main.c worker.c kafka.c public.c
	$(CC) $(CPPFLAGS) $(CFLAGS) main.c worker.c kafka.c public.c -o $@ $(LDFLAGS) $(LIBS)

clean:
	rm -f tcp2kafka *.o
