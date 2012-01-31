MESSAGEUTIL=bson.cma
CONNECTIONUTIL=bson.cma unix.cma message.cma
DIRPATH=-I ../util -I ../bson
OCAMLC=ocamlc -warn-error A -annot
OCAML=/opt/local/bin/rlwrap ocaml

all: message connection

connection: message
	${OCAMLC} ${DIRPATH} -a -o connection.cma ${CONNECTIONUTIL}  connection.mli connection.ml

message:
	${OCAMLC} ${DIRPATH} -a -o message.cma ${MESSAGEUTIL} message.mli message.ml


clean:
	rm -f *.cmo *.cmi  *.cmx *.o *.a *.spot *.spit *.annot *.cma *.cmxa


repl: all
	${OCAML} -I ../bson bson.cma connection.cma
