.PHONY: all
SUBDIR=util bson client

all:
	-for d in ${SUBDIR}; do (cd $$d; $(MAKE)); done;

clean:
	-for d in ${SUBDIR}; do (cd $$d; $(MAKE) clean); done;

