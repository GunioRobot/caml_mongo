let _stdout = stdout
open Unix
module Message = Message

type connection = {
  socket:file_descr;
  socket_addr:sockaddr
}

type response_header = {
  mlen:int32;
  response_to:int32;
  req_id:int32;
  opcode:int32
}

exception Connection_not_created
exception Cannot_connect
exception Write_failed
exception Bad_response

let create_connection ?(port = 27017) ~hostname = 
  try
    let sock_addr = ADDR_INET ((gethostbyname hostname).h_addr_list.(0), port) in
    let sock = socket PF_INET SOCK_STREAM 0 in
    connect sock sock_addr;
    {socket = sock; socket_addr = sock_addr}
  with e -> raise Connection_not_created

let send_message conn msg = 
  let mlen = (String.length msg) in
  let bytes_written = write conn.socket msg 0 (String.length msg) in
  if mlen > bytes_written then
    raise Write_failed
  else ()

let parse_response_header header =
  let readat = Binary.unpack_signed_32 ~buf:header in
  {
    mlen = readat ~pos:0;
    req_id = readat ~pos:4;
    response_to = readat ~pos:8;
    opcode = readat ~pos:12
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
      let response_header = parse_response_header header_buffer in
      if response_header.response_to = req_id then begin
          Printf.printf "%i bytes should be read\n" 
            (Int32.to_int (response_header.mlen));
          header_buffer ^ 
          (read_message_body (Int32.sub response_header.mlen 16l) sock);
        end
      else
        raise Bad_response
    | _ -> raise Bad_response in
  ignore (send_message conn msg);
  read_response ();

