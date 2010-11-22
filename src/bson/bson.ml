(* make sure to update the string value. 
 * I need to use UTF-8 encoding*)

module S = Xstring

type key = string
and value = Double of float
          | String of string
          | Document of document 
          | Array of value array
          | ObjectId of string
          | BinaryData of (binary_subtype * string) 
          (*char is the subtype, string is the
            bytes of data  *)
          | Boolean of bool
          | UTCDatetime of int64
          | Null
          | Regex of (string * string)
          | JSCode of string
          | JSCodeWithScope of (string * document)
          | Symbol of string
          | Int32 of int32
          | Int64 of int64
          | Timestamp of int64
          | MinKey
          | MaxKey

and document = (key * value) list

and binary_subtype = Generic | Function | GenericOld 
                   | UUID | MD5 | UserDefined of char

let _array_from_list lst =
  Array.of_list (List.map (fun (x, y) -> y) lst)

let rec document_to_bson doc =
  let bson_str = List.fold_left (fun s (k, v)  -> 
    s ^ element_to_bson (k  ^ "\x00") v) "" doc in
  Binary.pack_signed_32 (Int32.of_int (String.length bson_str + 4 + 1)) ^
  bson_str ^ "\x00"

and array_to_bson doc_ar = 
  let buf = ref "" in
  Array.iteri (fun i elt ->
    buf := !buf ^ element_to_bson ((string_of_int i) ^ "\x00") elt) doc_ar;
  Binary.pack_signed_32 (Int32.of_int (String.length !buf + 4 + 1)) ^
  !buf ^ "\x00"

and element_to_bson ename evalue =
  match evalue with
  | Double f ->
      "\x01" ^ ename ^ Binary.pack_float f
  | String s -> 
      "\x02" ^ ename ^ 
      Binary.pack_signed_32 (Int32.of_int (1 + String.length s)) ^ s ^ "\x00"
  | Document doc -> "\x03" ^ ename ^ document_to_bson doc
  | Array doc_ar -> "\x04" ^ ename ^ array_to_bson doc_ar 
  | BinaryData(subtype, bytes) ->
      "\x05" ^ ename ^ 
      Binary.pack_signed_32 (Int32.of_int (String.length bytes)) ^
      (match subtype with 
      | Generic -> "\x00"
      | Function -> "\x01"
      | GenericOld -> "\x02"
      | UUID -> "\x03"
      | MD5 -> "\x05"
      | UserDefined c -> String.make 1 c) ^
      bytes
  | ObjectId s ->
      assert (String.length s = 12);
      "\x07" ^ ename ^ s
  | Boolean b ->
      "\x08" ^ ename ^ if b then "\x01" else "\x00"
  | UTCDatetime i ->
      "\x09" ^ ename ^ Binary.pack_signed_64 i
  | Null -> "\x0A" ^ ename
  | Regex (regex, opt) -> "\x0B" ^ ename ^ regex ^ opt
  | JSCode code -> "\x0D" ^ ename ^
      Binary.pack_signed_32 (Int32.of_int (1 + String.length code)) ^ code ^ "\x00"
  | Symbol s -> "\x0E" ^ ename ^ 
      Binary.pack_signed_32 (Int32.of_int (1 + String.length s)) ^ s ^ "\x00"
  | JSCodeWithScope (code, mapping) -> 
      let mapping_binary = document_to_bson mapping in
      let code_len = Int32.of_int (1 + String.length code) in
      let mapping_len = Int32.of_int (String.length mapping_binary) in
      let total_len = Int32.add (Int32.of_int 8) (Int32.add code_len
                      mapping_len) in
      "\x0F" ^ ename ^
      Binary.pack_signed_32 total_len ^
      Binary.pack_signed_32 code_len ^ code ^ "\x00" ^
      mapping_binary
  | Int32 i ->
      "\x10" ^ ename ^
      Binary.pack_signed_32 i
  | Timestamp t ->
      "\x11" ^ ename ^
      Binary.pack_signed_64 t
  | Int64 i ->
      "\x12" ^ ename ^
      Binary.pack_signed_64 i
  | MinKey -> "\xFF" ^ ename
  | MaxKey -> "\x7F" ^ ename


exception Bson_decode_failure

let _extract_ename buf = 
  match S.split ~k:1 buf '\x00' with
  | [ename; rest] -> (ename, rest)
  | _ -> raise Bson_decode_failure

let rec _bson_to_element buf acc = 
  if buf = "" then List.rev acc
  else (
    let ename, rest =  _extract_ename (S.drop buf 1) in
    match buf.[0] with 
    | '\x01' (* double *) ->
        let newacc = (ename,
                      Double (Binary.unpack_float ~buf:(S.take rest 8) ~pos:0)):: acc in
        _bson_to_element (S.drop rest 8) newacc
    | '\x02' (* string, need to change to UTF-8 *) ->
        let str_size = Int32.to_int 
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let newacc = (ename, String (S.sub rest 4 (str_size - 1))) :: acc in
        _bson_to_element (S.drop rest (4 + str_size)) newacc
    | '\x03' (* nested document *) ->
        let doc_size = Int32.to_int
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let embedded_doc = _bson_to_list (S.take rest doc_size) in
        _bson_to_element (S.drop rest doc_size)
          ((ename, Document embedded_doc) :: acc)
    | '\x04' (* array *) ->
        let ar_size = Int32.to_int
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let ar = _array_from_list (_bson_to_list (S.take rest ar_size)) in
         _bson_to_element (S.drop rest ar_size)
          ((ename, Array ar) :: acc)
    | '\x05' (* binary *) ->
        let num_bytes = Int32.to_int
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let subtype = match rest.[4] with
          | '\x00' -> Generic
          | '\x01' -> Function
          | '\x02' -> GenericOld
          | '\x03' -> UUID
          | '\x05' -> MD5
          | c ->  UserDefined c in
        let newacc = (ename, 
          BinaryData(subtype, S.sub rest 5 num_bytes)) :: acc in
        _bson_to_element (S.drop rest (5 + num_bytes)) newacc
    | '\x07' (* Object Id *) ->
        assert (S.length rest >= 12);
        _bson_to_element (S.drop rest 12)
          ((ename, ObjectId (S.take rest 12)) :: acc)
    | '\x08' (* bool *) -> 
        let b = match rest.[0] with
          | '\x00' -> false
          | '\x01' -> true
          | _ -> raise Bson_decode_failure in
        _bson_to_element (S.drop rest 1) ((ename, Boolean b) :: acc)
    | '\x09' (* UTC datetime *) ->
        _bson_to_element (S.drop rest 4)
          ((ename, UTCDatetime (Binary.unpack_signed_64 
            ~buf:(S.take rest 4) ~pos:0)) :: acc)
    | '\x0A' (* NULL value *) ->
       _bson_to_element rest ((ename, Null) :: acc) 
    | '\x0B' (* regex *) -> (
        match S.split ~k:2 rest '\x00' with
        | pat :: opt :: [new_rest] -> 
          _bson_to_element new_rest ((ename, Regex(pat, opt)) :: acc)
        | _ -> raise Bson_decode_failure)
    | '\x0D' (* JS code *) ->
        let code_size = Int32.to_int 
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let newacc = (ename, JSCode (S.sub rest 4 (code_size - 1))) :: acc in
        _bson_to_element (S.drop rest (4 + code_size)) newacc
    | '\x0E' (* symbol *) ->
        let str_size = Int32.to_int 
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let newacc = (ename, Symbol (S.sub rest 4 (str_size - 1))) :: acc in
        _bson_to_element (S.drop rest (4 + str_size)) newacc
    | '\x0F' (* JS code with scope *) ->
        let elt_size = Int32.to_int
          (Binary.unpack_signed_32 ~buf:(S.take rest 4) ~pos:0) in
        let code_size = Int32.to_int
          (Binary.unpack_signed_32 ~buf:(S.sub rest 4 4) ~pos:0) in
        let code = S.sub rest 8 (code_size - 1) in
        let scope_doc = _bson_to_list 
          (S.sub rest (8 + code_size) (elt_size - 8 -code_size)) in
        _bson_to_element (S.drop rest elt_size)
          ((ename, JSCodeWithScope(code, scope_doc)) :: acc)
    | '\x10' (* int32 *) ->
        let newacc = (ename, Int32 (Binary.unpack_signed_32 
          ~buf:(S.take rest 4) ~pos:0)):: acc in
        _bson_to_element (S.drop rest 4) newacc
    | '\x11' (* timestamp *) ->
        let newacc = (ename, Timestamp (Binary.unpack_signed_64 
          ~buf:(S.take rest 8) ~pos:0)):: acc in
        _bson_to_element (S.drop rest 8) newacc
    | '\x12' (* int64 *) ->
        let newacc = (ename, Int64 (Binary.unpack_signed_64 
          ~buf:(S.take rest 8) ~pos:0)):: acc in
        _bson_to_element (S.drop rest 8) newacc
    | '\xFF' (* min key *) ->
        _bson_to_element rest ((ename, MinKey) :: acc)
    | '\x7F' (* max key *) ->
        _bson_to_element rest ((ename, MaxKey) :: acc)
    | _ -> raise Bson_decode_failure
  )

and _bson_to_list buf = 
  if S.length buf < 5 || buf.[(S.length buf) - 1] <> '\x00'
  then raise Bson_decode_failure
  else (
    (*I should check that the header describes the correct number of bytes*)
      (_bson_to_element (S.drop (S.drop buf 4) (-1)) [])
  )

let bson_to_document = _bson_to_list

let bson_to_multidocs bson_str =
  let rec process buf acc =
    match buf with
    | "" -> acc
    | _ -> 
        let len = Int32.to_int (Binary.unpack_signed_32 ~buf:(S.take buf 4) ~pos:0) in
        process (S.drop buf len) ((bson_to_document (S.take buf len))::acc)
  process bson_str []
