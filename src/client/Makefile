MESSAGEUTIL=bson.cma
CONNECTIONUTIL=bson.cma unix.cma message.cma
DBUTIL=connection.cmo
DIRPATH=-I ../util -I ../bson
OCAMLC=ocamlc -warn-error A
OCAML=/opt/local/bin/rlwrap ocaml

all: message connection db

connection: message
	${OCAMLC} ${DIRPATH} -a -o connection.cma ${CONNECTIONUTIL} connection.mli connection.ml

message:
	${OCAMLC} ${DIRPATH} -a -o message.cma ${MESSAGEUTIL} message.mli message.ml

db: connection
	${OCAMLC} ${DIRPATH} -a -o db.cma ${DBUTIL} db.ml

clean:
	rm -f *.cmo *.cmi  *.cmx *.o *.a *.spot *.spit *.annot *.cma *.cmxa


repl: all
	${OCAML} -I ../bson bson.cma connection.cma
