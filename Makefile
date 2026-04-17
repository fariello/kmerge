CC = gcc
CFLAGS = -Wall -Wextra -O3 -march=native -flto -funroll-loops -std=c11 -D_POSIX_C_SOURCE=200809L

TARGET = kmerge
SRCS = kmerge.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
