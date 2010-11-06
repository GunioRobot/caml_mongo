(* module to create messages *)
open Binary
module S = Xstring
module Bson = Bson

let zero = pack_signed_32 0l

let create_header ~mlen ~req_id ~req_to ~opcode =
  (pack_signed_32 mlen) ^
  (pack_signed_32 req_id) ^
  (pack_signed_32 req_to) ^
  (pack_signed_32 opcode)

let _update opcode coll_name flags selector update =
  let selector_bson = Bson.document_to_bson selector in
  let update_bson = match update with
    | Some u -> Bson.document_to_bson u 
    | None -> "" in
  let coll_cstr = S.to_c_string coll_name in
  let flags = pack_signed_32 flags in
  let req_id = 0l (* just temorarily *) in
  (* 16 header 4 zero 4 flags *)
  let mlength = Int32.of_int (
    16 + 4 + S.length coll_cstr + 4 + S.length selector_bson +
    S.length update_bson ) in
  let header = create_header ~mlen:mlength ~req_id:req_id ~req_to:0l
                             ~opcode:opcode in
  header ^ zero ^ coll_cstr ^ flags ^ selector_bson ^ update_bson

let update ~coll_name ~flags ~selector ~update =
  _update 2001l coll_name flags selector (Some update)

let delete ~coll_name ~flags ~selector =
  _update 2006l coll_name flags selector None

let insert ~coll_name ~docs = 
  let doc_bson = List.fold_left (fun x y -> x ^ Bson.document_to_bson y) 
                                ""  docs in
  let coll_cstr = S.to_c_string coll_name in
  (* 16 is for the header, 4 is for the zero *)
  let mlength = Int32.of_int (16 + 4 + S.length coll_cstr + S.length doc_bson) in
  let req_id = 0l (* just temporarily *) in
  let opcode = 2002l in
  let header = create_header ~mlen:mlength ~req_id:req_id ~req_to:0l
                             ~opcode:opcode in
  header ^ zero ^ coll_cstr ^ doc_bson

