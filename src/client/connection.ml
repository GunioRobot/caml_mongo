module Message = Message
module Bson = Message.Bson

open Unix
type connection = {
  socket:file_descr;
  socket_addr:sockaddr
}

exception Connection_not_created
exception Cannot_connect
exception Write_failed

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

