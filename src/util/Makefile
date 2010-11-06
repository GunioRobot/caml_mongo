SRC=binary.ml xstring.ml

all: native bytecode

native:
	ocamlopt -c ${SRC}

bytecode:
	ocamlc -c ${SRC}

clean:
	rm -f *.cmo *.cmi *.cmx *.o
