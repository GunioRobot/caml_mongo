val update : coll_name:string -> flags:int32 -> selector:Bson.document ->
             update:Bson.document -> string
val delete : coll_name:string -> flags:int32 -> selector:Bson.document ->
             string
val insert : coll_name:string -> docs:Bson.document list -> string
val query : ?ret_field_selector:Bson.document -> coll_name:string ->
            flags:int32 -> num_skip:int32 -> num_rtn:int32 ->
            query:Bson.document -> string
