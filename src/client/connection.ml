open Unix
module Bson = Bson
module S = Xstring

type t = {
  socket:file_descr;
  socket_addr:sockaddr
}

type connection_pool = t list

type reply_header = {
  mlen:int32;
  response_to:int32;
  req_id:int32;
  opcode:int32
}

type reply_body = {
  response_flag:int32;
  cursor_id:int64;
  starting_from:int32;
  num_docs:int32;
  docs:Bson.document list
}

type reply = {
  header:reply_header;
  body:reply_body
}

let create_connection ?(port = 27017) ~hostname = 
  try
    let sock_addr = ADDR_INET ((gethostbyname hostname).h_addr_list.(0), port) in
    let sock = socket PF_INET SOCK_STREAM 0 in
    connect sock sock_addr;
    Some {socket = sock; socket_addr = sock_addr}
  with e -> None

let create_connection_pool ?(port = 27017) ?(num_conn = 10) ~hostname =
  let rec aux i pool = 
    if i = num_conn then pool
    else begin
      let newpool = match create_connection ~port:port ~hostname with
        | Some conn -> conn :: pool
        | None -> pool in
      aux (i + 1) newpool
    end in
  aux 0 []

let delete_connection_pool pool = 
  List.iter (fun conn -> shutdown conn.socket SHUTDOWN_ALL) pool

module Communicate = struct
  let send_message conn msg = 
    let mlen = (String.length msg) in
    let bytes_written = write conn.socket msg 0 (String.length msg) in
    if mlen > bytes_written then
      failwith "write to socket failed in send_message"
    else ()

  let parse_reply_header header =
    let readat = Binary.unpack_signed_32 ~buf:header in
    {
      mlen = readat ~pos:0;
      req_id = readat ~pos:4;
      response_to = readat ~pos:8;
      opcode = readat ~pos:12
    }

  let parse_reply_body body =
    let readat32 = Binary.unpack_signed_32 ~buf:body in
    let readat64 = Binary.unpack_signed_64 ~buf:body in
    {
      response_flag = readat32 ~pos:0;
      cursor_id = readat64 ~pos:4;
      starting_from = readat32 ~pos:12;
      num_docs = readat32 ~pos:16;
      docs = Bson.bson_to_multidocs (S.drop body 20)
    }

  let read_message_body mlen sock = 
    let buffer_size = 4096 in
    let buffer = String.create 4096 in
    let rec reader msg_sofar n =
      if n >= mlen then msg_sofar
      else begin 
        let bytes_read = read sock buffer 0 buffer_size in
        reader (msg_sofar ^ (String.sub buffer 0 bytes_read))
          (Int32.add n (Int32.of_int bytes_read))
      end in
    reader "" 0l

  let send_and_receive_message conn msg req_id =
    let sock = conn.socket in 
    let read_response () =
      let header_buffer = String.create 16 in
      match read sock header_buffer 0 16 with
      | 16 ->
        let reply_header = parse_reply_header header_buffer in
        if reply_header.response_to = req_id then begin
            let reply_body = parse_reply_body (read_message_body 
                              (Int32.sub reply_header.mlen 16l) sock) in
            {header = reply_header; body = reply_body}
          end
        else
          failwith "the response's request id did not 
                    match the original request id"
      | _ -> failwith "failed to read the header" in
    ignore (send_message conn msg);
    read_response ();
end
;;

(*val update : coll_name:string -> flags:int32 -> selector:Bson.document ->
             update:Bson.document -> string*)
let update ?(upsert = false) ?(multi = false) ~conn ~coll_name ~selector ~update =
  let flags = match upsert, multi with
  | false, false -> 0l
  | true, false -> 1l
  | false, true -> 2l
  | true, true -> 3l in
  let msg = Message.update ~coll_name:coll_name ~flags:flags 
              ~selector:selector ~update:update in
  Communicate.send_message conn msg

    
    
  

