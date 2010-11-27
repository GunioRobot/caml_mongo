type t
type cursor

(*
type reply_header = {
  mlen:int32;
  response_to:int32;
  req_id:int32;
  opcode:int32
}

type reply_meta = {
  cursor_not_found:bool;
  query_failure:bool;
  shard_config_stale:bool;
  await_capable:bool
}

type reply_body = {
  response_flags:reply_meta;
  cursor_id:int64;
  starting_from:int32;
  num_docs:int32;
  docs:Bson.document Stream.t
}

type cursor = {
  conn:t;
  coll_name:string;
  mutable reply:reply_body
}
*)

module Cursor : sig
  type t = cursor
  type cursor_val = Done | Val of Bson.document
  val next : t -> cursor_val
end

type delete_option = DeleteAll | DeleteOne
type command_status = Succeeded | Failed

val create_connection : 
  ?num_conn:int -> ?port:int -> string -> t

val update :
  ?upsert:bool -> ?multi:bool -> conn:t ->coll_name:string -> 
    Bson.document -> Bson.document -> command_status
val insert :
  conn:t -> coll_name:string -> Bson.document list -> command_status 
val delete :
  ?delete_option:delete_option -> conn:t -> coll_name:string -> 
    Bson.document -> command_status
(* there are actually many options on find, but for now I am ignoring
 * all of them*)
val find :
  ?ret_field_selector:Bson.document -> conn:t -> coll_name:string ->
    Bson.document -> cursor option
