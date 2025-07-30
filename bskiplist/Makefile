OPT?=fast
VALGRIND?=0
INFO?=0
ENABLE_TRACE_TIMER?=0
CYCLE_TIMER?=0
DEBUG?=0
CILK?=0
SANITIZE?=0
CORRECTNESS?=0
LATENCY?=0
STATS?=0

# CXX=clang++
# CFLAGS := -Wall -Wextra -O$(OPT) -g  -std=c++20 -IParallelTools/ -I../ -gdwarf-4
CFLAGS := -pthread -Wall -Wextra -std=c++20 -IParallelTools/ -Wno-deprecated-declarations -Iparlaylib/include/ -Wshadow -fmax-errors=1

ifeq ($(DEBUG),1)
CFLAGS += -g -gdwarf-4 -O0
else
CFLAGS += -O$(OPT)
endif

ifeq ($(SANITIZE),1)
ifeq ($(CILK),1)
CFLAGS += -fsanitize=cilk,undefined,address -fno-omit-frame-pointer
else
CFLAGS += -fsanitize=undefined,address -fno-omit-frame-pointer
endif
endif

LDFLAGS := -lrt -lm -lpthread -lm -ldl -latomic

ifeq ($(VALGRIND),0)
CFLAGS += -march=native
endif

DEFINES := -DENABLE_TRACE_TIMER=$(ENABLE_TRACE_TIMER) -DCYCLE_TIMER=$(CYCLE_TIMER) -DCILK=$(CILK) -DDEBUG=$(DEBUG) -DCORRECTNESS=$(CORRECTNESS)

ifeq ($(CILK),1)
CFLAGS += -fopencilk -DPARLAY_CILK
ONE_WORKER = CILK_NWORKERS=1
endif

ifeq ($(DEBUG),0)
CFLAGS += -DNDEBUG
endif


ifeq ($(INFO), 1)
# CFLAGS +=  -Rpass-missed="(inline|loop*)"
#CFLAGS += -Rpass="(inline|loop*)" -Rpass-missed="(inline|loop*)" -Rpass-analysis="(inline|loop*)"
CFLAGS += -Rpass=.* -Rpass-missed=.* -Rpass-analysis=.*
endif

ifeq ($(LATENCY), 1)
CFLAGS += -DLATENCY=1
endif

ifeq ($(STATS), 1)
DEFINES += -DSTATS=1
endif


all: ycsb

ycsb: ycsb.cpp bskip.h
	$(CXX) $(CFLAGS) $(DEFINES) -o ycsb ycsb.cpp $(LDFLAGS)

test: test.cpp bskip.h
	$(CXX) $(CFLAGS) $(DEFINES) -o $@ test.cpp $(LDFLAGS)

clean:
	rm -rf basic test ycsb
