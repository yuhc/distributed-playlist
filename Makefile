CC = g++

ODIR = ./obj
output_dir := $(shell mkdir -p $(ODIR))
SRC = process
PROG = process

EXEFILE = $(ODIR)/$(PROG)

OFILES = $(patsubst %, $(ODIR)/%.o, $(SRC))
DEPS = $(patsubst %, $(ODIR)/%.d, $(SRC))

.DEFAULT_GOAL = all
.PHONY: all clean
.PRECIOUS: $(OFILES)

CXXFLAGS = -O3 -ggdb -MMD -std=c++11
LDFLAGS = -pthread

all: $(EXEFILE)

clean:
	rm -rf $(ODIR)

rmlog:
	rm -rf log_*

$(ODIR)/%.o: %.cc
	$(CC) $< -o $@ -c $(CXXFLAGS)

$(EXEFILE): $(OFILES)
	$(CC) $^ -o $@ $(LDFLAGS)

-include $(DEPS)
