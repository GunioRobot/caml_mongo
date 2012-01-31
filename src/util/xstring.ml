(* a few additional functions *)
include String

let rev s =
  let n = length s in
  let buf = create n in
  for i = 0 to (n - 1) do
    buf.[i] <- s.[n - i - 1]
  done;
  buf

let sub_noexn s i len = try sub s i len with e -> ""

let index_noexn s c = try Some (index s c) with e -> None

let split ?(k = -1) s delim =
  let rec loop s d acc len k =
    match index_noexn s d with
    | Some i when k <> 0  ->
        loop (sub_noexn s (i + 1) (len - i - 1)) d
             ((sub_noexn s 0 i) :: acc) (len - i - 1) (k - 1)
    | _ -> List.rev (s :: acc) in
  loop s delim [] (length s) k

let take s i = sub_noexn s 0 i
let drop s n =
  if n >= 0 then sub_noexn s n ((length s) - n)
  else sub_noexn s 0 ((length s) + n)

let to_c_string s = s ^ "\x00"
