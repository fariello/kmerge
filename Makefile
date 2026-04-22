CC = gcc
CFLAGS = -Wall -Wextra -O3 -march=x86-64 -flto -funroll-loops -std=c11 -D_POSIX_C_SOURCE=200809L

WARNFLAGS = -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion \
            -Wformat=2 -Wstrict-prototypes -Wmissing-prototypes \
            -Wnull-dereference -Wdouble-promotion -Wundef \
            -fanalyzer -O1 -std=c11 -D_POSIX_C_SOURCE=200809L

TARGET = kmerge
SRCS = kmerge.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS)

# Run an extended warning audit without producing a binary.
warn: $(SRCS)
	$(CC) $(WARNFLAGS) -o /dev/null $(SRCS)

clean:
	rm -f $(TARGET)

.PHONY: all clean warn
