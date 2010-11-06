MESSAGEUTIL=xstring.cmo binary.cmo bson.cmo
CONNECTIONUTIL=unix.cma message.cmo
DIRPATH=-I ../util -I ../bson
OCAMLC=ocamlc
OCAML=ocaml

all: message connection

connection: message
	${OCAMLC} ${DIRPATH} -a -o connection.cma ${CONNECTIONUTIL} connection.ml

message:
	${OCAMLC} ${DIRPATH} -a -o message.cma ${MESSAGEUTIL} message.ml
clean:
	rm -f *.cmo *.cmi  *.cmx *.o *.a *.spot *.spit *.annot *.cma *.cmxa
