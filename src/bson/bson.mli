type value =
| Double of float
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
and document = (string * value) list
and binary_subtype = Generic | Function | GenericOld 
                   | UUID | MD5 | UserDefined of char

val document_to_bson : document -> string
val bson_to_document : string -> document
val bson_to_multidocs : string -> document list
