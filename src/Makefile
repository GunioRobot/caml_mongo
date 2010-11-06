all: makeclient

makeutil:
	cd util; make
cleanutil:
	cd util; make clean

makebson: makeutil
	cd bson; make
cleanbson:
	cd bson; make clean

makeclient: makebson
	cd client; make
cleanclient:
	cd client; make clean

clean: cleanutil cleanbson cleanclient

