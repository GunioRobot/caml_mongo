(* module to create messages *)
open Binary
module S = Xstring

let zero = pack_signed_32 0l

let create_header ~mlen ~request_id ~response_to ~opcode =
  (pack_signed_32 mlen) ^
  (pack_signed_32 request_id) ^
  (pack_signed_32 response_to) ^
  (pack_signed_32 opcode)

let _update opcode coll_name flags selector update =
  let selector_bson = Bson.document_to_bson selector in
  let update_bson = match update with
    | Some u -> Bson.document_to_bson u 
    | None -> "" in
  let coll_cstr = S.to_c_string coll_name in
  let flags = pack_signed_32 flags in
  (* 16 header 4 zero 4 flags *)
  let mlength = Int32.of_int (
    16 + 4 + S.length coll_cstr + 4 + S.length selector_bson +
    S.length update_bson ) in
  let header = create_header ~mlen:mlength ~request_id:0l ~response_to:0l
                             ~opcode:opcode in
  header ^ zero ^ coll_cstr ^ flags ^ selector_bson ^ update_bson

let update ~coll_name ~flags ~selector ~update =
  _update 2001l coll_name flags selector (Some update)

(* delete is just update without any update *)
let delete ~coll_name ~flags ~selector =
  _update 2006l coll_name flags selector None

let insert ~coll_name ~docs = 
  let doc_bson = List.fold_left (fun x y -> x ^ Bson.document_to_bson y) 
                                ""  docs in
  let coll_cstr = S.to_c_string coll_name in
  (* 16 is for the header, 4 is for the zero *)
  let mlength = Int32.of_int (16 + 4 + S.length coll_cstr + S.length doc_bson) in
  let header = create_header ~mlen:mlength ~request_id:0l ~response_to:0l
                             ~opcode:2002l in
  header ^ zero ^ coll_cstr ^ doc_bson

let query ~ret_field_selector ~coll_name ~flags ~num_skip 
          ~num_rtn ~query =
  let query_bson = Bson.document_to_bson query in
  let ret_field_selector_bson = Bson.document_to_bson ret_field_selector in
  let coll_cstr = S.to_c_string coll_name in
  let num_skip = pack_signed_32 num_skip in
  let num_rtn = pack_signed_32 num_rtn in
  let flags = pack_signed_32 flags in
  let mlength = Int32.of_int (16 + 4 + S.length coll_cstr + 
                              4 + 4 + S.length query_bson +
                              S.length ret_field_selector_bson) in
  let header = create_header ~mlen:mlength ~request_id:0l ~response_to:0l
                             ~opcode:2004l in
  header ^ flags ^ coll_cstr ^ num_skip ^ num_rtn ^ query_bson ^ 
  ret_field_selector_bson

let getmore ~coll_name ~num_rtn ~cursor_id =
  let coll_cstr = S.to_c_string coll_name in
  let num_rtn = pack_signed_32 num_rtn in
  let cursor_id = pack_signed_64 cursor_id in
  let mlength = Int32.of_int (16 + 4 + S.length coll_cstr + 4 + 8) in
  let header = create_header ~mlen:mlength ~request_id:0l ~response_to:0l
                             ~opcode:2005l in
  header ^ zero ^ coll_cstr ^ num_rtn ^ cursor_id

let kill_cursors cursor_ids = 
  let num_cursor = Int32.of_int (List.length cursor_ids) in
  let num_cursor_packed = pack_signed_32 num_cursor  in
  let cursors = List.fold_left (fun x y -> x ^ pack_signed_64 y) 
                  "" cursor_ids in
  let mlength = Int32.add (Int32.of_int (16 + 4 + 4 ))
                  (Int32.mul 8l num_cursor) in
  let header = create_header ~mlen:mlength ~request_id:0l ~response_to:0l
                             ~opcode:2007l in
  header ^ zero ^ num_cursor_packed ^ cursors
