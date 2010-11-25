open Binary
open Unix
module S = Xstring

type connection = {
  socket:file_descr;
  socket_addr:sockaddr;
}
type t (* this is the connection pool *) = {
  num_conn:int;
  pool:connection option array;
}
type reply_header = {
  mlen:int32;
  response_to:int32;
  req_id:int32;
  opcode:int32;
}
type reply_meta = {
  cursor_not_found:bool;
  query_failure:bool;
  shard_config_stale:bool;
  await_capable:bool;
}
type reply_body = {
  response_flags:reply_meta;
  cursor_id:int64;
  starting_from:int32;
  num_docs:int32;
  docs:Bson.document Stream.t;
}
type cursor = {
  conn:t;
  coll_name:string;
  mutable reply:reply_body;
}
type delete_option = DeleteAll | DeleteOne

let create_single_connection ?(port = 27017) hostname = 
  try
    let sock_addr = ADDR_INET ((gethostbyname hostname).h_addr_list.(0), port) in
    let sock = socket PF_INET SOCK_STREAM 0 in
    connect sock sock_addr;
    Some {socket = sock; socket_addr = sock_addr}
  with e -> None

let create_connection ?(num_conn = 10) ?(port = 27017) hostname =
  Random.self_init (); (* this is going to be used for pick_connection *)
  let pool = Array.init num_conn
    (fun _ -> create_single_connection ~port:port hostname) in
  Random.self_init ();
  {
    num_conn = num_conn;
    pool = pool;
  }

let pick_connection conn_pool = 
  let n = conn_pool.num_conn - 1 in
  let i = Random.int n in
  match conn_pool.pool.(i) with
  | Some c -> c
  | None -> failwith "found a dropped connection"

let delete_connection_pool pool = 
  Array.iter (fun conn -> shutdown conn.socket SHUTDOWN_ALL) pool
module Communicate = struct
  let send_message conn msg = 
    let mlen = (String.length msg) in
    let bytes_written = write conn.socket msg 0 (String.length msg) in
    if mlen > bytes_written then
      failwith "write to socket failed in send_message"
    else ()

  let parse_reply_header header =
    let readat = unpack_signed_32 ~buf:header in
    {
      mlen = readat ~pos:0;
      req_id = readat ~pos:4;
      response_to = readat ~pos:8;
      opcode = readat ~pos:12
    }

  let parse_response_flags f =
    {
      cursor_not_found = Int32.logand f 1l = 1l;
      query_failure = Int32.logand f 2l = 2l;
      shard_config_stale = Int32.logand f 4l = 4l;
      await_capable = Int32.logand f 8l = 8l;
    }

  let parse_reply_body body =
    let readat32 = unpack_signed_32 ~buf:body in
    let readat64 = unpack_signed_64 ~buf:body in
    {
      response_flags = parse_response_flags (readat32 ~pos:0);
      cursor_id = readat64 ~pos:4;
      starting_from = readat32 ~pos:12;
      num_docs = readat32 ~pos:16;
      docs = Stream.of_list (Bson.bson_to_multidocs (S.drop body 20))
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
        if reply_header.response_to = req_id then
            parse_reply_body (read_message_body 
              (Int32.sub reply_header.mlen 16l) sock)
        else
          failwith "the response's request id did not 
                    match the original request id"
      | _ -> failwith "failed to read the header" in
    ignore (send_message conn msg);
    read_response ();
end

module Cursor = struct
  type t = cursor
  type cursor_val = Done | Val of Bson.document

  let kill_cursor c =
    let msg = Message.kill_cursors [c.reply.cursor_id] in
    Communicate.send_message (pick_connection c.conn) msg

  let next c =
    if c.reply.response_flags.cursor_not_found
    then begin
      kill_cursor c;
      Done;
    end
    else begin
      try Val (Stream.next c.reply.docs)
      with e ->
        let conn = pick_connection c.conn in
        let msg = Message.getmore ~coll_name:c.coll_name 
                    ~num_rtn:10l ~cursor_id:c.reply.cursor_id in
        c.reply <- Communicate.send_and_receive_message conn msg 0l;
        try Val (Stream.next c.reply.docs)
        with e ->
          kill_cursor c;
          Done;
    end
end


let update ?(upsert = false) ?(multi = false) ~conn ~coll_name selector update =
  let conn = pick_connection conn in
  let flags = match upsert, multi with
  | false, false -> 0l
  | true, false -> 1l
  | false, true -> 2l
  | true, true -> 3l in
  let msg = Message.update ~coll_name:coll_name ~flags:flags 
              ~selector:selector ~update:update in
  Communicate.send_message conn msg

let insert ~conn ~coll_name ~docs =
  let conn = pick_connection conn in
  let msg = Message.insert ~coll_name:coll_name ~docs:docs in
  Communicate.send_message conn msg

let delete ?(delete_option = DeleteAll) ~conn ~coll_name selector =
  let conn = pick_connection conn in
  let flags = (function DeleteAll -> 0l | DeleteOne -> 1l) delete_option in
  let msg = Message.delete ~coll_name:coll_name ~flags:flags ~selector in
  Communicate.send_message conn msg

let find ?(ret_field_selector = []) ~conn ~coll_name selector = 
  let single_conn = pick_connection conn in
  let msg = Message.query ~ret_field_selector:ret_field_selector
              ~coll_name:coll_name ~query:selector ~flags:0l
              ~num_skip:0l ~num_rtn:10l in 
  let reply_body = Communicate.send_and_receive_message single_conn msg 0l in 
  {
    conn = conn; 
    coll_name = coll_name; 
    reply = reply_body
  }

