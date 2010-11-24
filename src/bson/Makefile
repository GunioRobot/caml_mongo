all: bytecode

nativecode:
	ocamlopt -warn-error A -I ../util -a -o bson.cmxa xstring.cmx binary.cmx bson.mli bson.ml

bytecode:
	ocamlc -warn-error A -I ../util -o bson.cma -a ../util/xstring.cmo ../util/binary.cmo bson.mli bson.ml

clean:
	rm -f *.cmo *.cmi  *.cmx *.o *.a *.spot *.spit *.annot *.cma *.cmxa
