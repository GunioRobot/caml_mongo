.PHONY: all native bytecode clean
SRC=binary.ml xstring.ml

all: bytecode

native:
	ocamlopt -c ${SRC}

bytecode:
	ocamlc -c -annot -warn-error A ${SRC}

clean:
	rm -f *.cmo *.cmi *.cmx *.o
