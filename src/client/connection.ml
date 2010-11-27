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
  hostname:string;
  port:int;
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
type command_status = Succeeded | Failed


let create_single_connection ?(port = 27017) hostname = 
  try
    let sock_addr = ADDR_INET ((gethostbyname hostname).h_addr_list.(0), port) in
    let sock = socket PF_INET SOCK_STREAM 0 in
    connect sock sock_addr;
    Some {socket = sock; socket_addr = sock_addr}
  with e -> None

let delete_connection conn =
  Array.iter (fun conn -> match conn with
    | Some c -> shutdown c.socket SHUTDOWN_ALL
    | None -> ()) conn.pool

let create_connection ?(num_conn = 10) ?(port = 27017) hostname =
  Random.self_init (); (* this is going to be used for pick_connection *)
  let pool = Array.init num_conn
    (fun _ -> create_single_connection ~port:port hostname) in
  let conn = {
    num_conn = num_conn;
    pool = pool;
    port = port;
    hostname = hostname;
  } in begin
    Gc.finalise (fun c -> delete_connection c) conn;
    conn;
  end

let pick_connection conn_pool = 
  let n = conn_pool.num_conn - 1 in
  let i = Random.int n in
  match conn_pool.pool.(i) with
  | Some c -> c
  | None -> begin
      match create_single_connection ~port:conn_pool.port conn_pool.hostname with
      | Some c as c_opt ->
          conn_pool.pool.(i) <- c_opt;
          c;
      | None -> failwith "Failed to connect!"
  end

module Communicate = struct
  let send_message conn msg = 
    let mlen = (String.length msg) in
    let bytes_written = write conn.socket msg 0 (String.length msg) in
    if mlen > bytes_written then
      Failed
    else Succeeded

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
          Some (parse_reply_body (read_message_body 
            (Int32.sub reply_header.mlen 16l) sock))
        else
          None
      | _ -> None in
    match send_message conn msg with
    | Failed -> None
    | Succeeded -> read_response ();
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
      ignore (kill_cursor c);
      Done;
    end
    else begin
      try 
        Val (Stream.next c.reply.docs)
      with e ->
        let conn = pick_connection c.conn in
        let msg = Message.getmore ~coll_name:c.coll_name 
                    ~num_rtn:10l ~cursor_id:c.reply.cursor_id in
        match Communicate.send_and_receive_message conn msg 0l with
        | Some r -> begin 
            c.reply <- r;
            try
              Val (Stream.next c.reply.docs);
            with e ->
              ignore (kill_cursor c);
              Done;
          end
        | None -> failwith "connection was dropped unexpectedly!"
    end
  
  let make_gcable c =
    Gc.finalise (fun x -> 
      match kill_cursor x with
      | Succeeded _ -> ()
      | Failed -> failwith "connection was dropped unexpectedly!") c;
    c;

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

let insert ~conn ~coll_name docs =
  match docs with
  | [] -> Succeeded
  | _ ->
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
  match Communicate.send_and_receive_message single_conn msg 0l with
  | None -> None
  | Some reply_body ->
    Some (
      Cursor.make_gcable {
          conn = conn; 
          coll_name = coll_name; 
          reply = reply_body;
      })

