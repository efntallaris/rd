# Static library librdma_migration.a -- the slot-keyed RDMA migration
# infrastructure used by Redis's cluster_rdma.c handlers.
#
# Inherits STD/WARN/OPT/DEBUG from the parent Redis Makefile when invoked
# recursively. Uses LIB_CFLAGS (not CFLAGS) so a parent-level CFLAGS=""
# override doesn't suppress -I.. and -fPIC.

LIB         = librdma_migration.a
SRCS        = rdma_buffer.c rdma_client.c rdma_server.c allocator.c
OBJS        = $(SRCS:.c=.o)

STD        ?= -std=c11
WARN       ?= -Wall -Wextra
OPT        ?= -O2
DEBUG      ?= -g

LIB_CFLAGS  = $(STD) $(WARN) $(OPT) $(DEBUG) -fPIC -I.. -Iinclude $(CFLAGS)

.PHONY: all clean
all: $(LIB)

$(LIB): $(OBJS)
	$(AR) rcs $@ $^

%.o: %.c
	$(CC) $(LIB_CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(LIB)
