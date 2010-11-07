MESSAGEUTIL=xstring.cmo binary.cmo bson.cmo
CONNECTIONUTIL=unix.cma binary.cmo message.cmo
DIRPATH=-I ../util -I ../bson
OCAMLC=ocamlc -warn-error A
OCAML=/opt/local/bin/rlwrap ocaml

all: message connection

connection: message
	${OCAMLC} ${DIRPATH} -a -o connection.cma ${CONNECTIONUTIL} connection.ml

message:
	${OCAMLC} ${DIRPATH} -a -o message.cma ${MESSAGEUTIL} message.mli message.ml
clean:
	rm -f *.cmo *.cmi  *.cmx *.o *.a *.spot *.spit *.annot *.cma *.cmxa


repl: all
	${OCAML} -I ../bson message.cma bson.cma connection.cma
