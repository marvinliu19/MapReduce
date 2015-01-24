open Async.Std

module Job = struct
  type input  = string * string list
  type key    = string * string (* Friendship *)
  type inter  = string
  type output = string list

  let name = "friends.job"

  (* maps inputs of a name and their friendlist to a key of 
    two people that are friends and a friend who is friends
    between the two of them. This friend is someone from the
    friendlist, and will include the second person in the key*)
  let map (name, friendlist) : (key*inter) list Deferred.t =
    (* This helper takes in a name and a friend, and uses the
       friendlist to generate inters (will include friend) *)
    let rec inter_maker acc name f flist =
      match flist with
      | h::t -> inter_maker (((name,f),h)::acc) name f t
      | [] -> acc
    in
    (* This folds over the friendlist, generating keys and 
    calling inter_maker*)
    return (
      List.fold_left 
        (fun acc friend -> 
          if (compare name friend)<0 then
            (inter_maker acc name friend friendlist) else 
            (inter_maker acc friend name friendlist)) 
        []
        friendlist)

  let reduce ((_, friendlists): key * inter list) : output Deferred.t =
    (* Uses the inter list and reduces it by finding duplicates in 
      the inter list, and eliminating one of the duplicates and 
      everything that isn't duplicated *)
    let rec reduce_helper acc flist = 
      match flist with
        | h1::t -> 
          if List.mem h1 t then 
          reduce_helper (h1::acc) t else 
          reduce_helper acc t
        | _ -> acc
    in return (reduce_helper [] friendlists)
end

let () = MapReduce.register_job (module Job)

let read_line (line:string) :(string * (string list)) =
  match Str.split (Str.regexp ":") line with
    | [person; friends] -> begin
      let friends = Str.split (Str.regexp ",") friends in
      let trimmed = List.map String.trim friends in
      (person, trimmed)
    end
    | _ -> failwith "Malformed input in graph file."

let read_files (files: string list) : ((string * (string list)) list) Deferred.t =
  match files with
  | []    -> failwith "No graph files provided."
  | files -> begin
    Deferred.List.map files Reader.file_lines
    >>| List.flatten
    >>| List.map read_line
  end

module App = struct
  let name = "friends"

  let print common_friends =
    let print_friends ((a, b), friends) =
      printf "(%s, %s): %s\n" a b (String.concat ", " friends)
    in
    List.iter print_friends common_friends

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (* You may assume that main is called with a single, valid graph file. You
       do not need to handle malformed input. For example graph files, see the
       data directory. *)
    let main args =
        read_files args
        >>= MR.map_reduce
        (* replace this failwith with print once you've figured out the key and
           inter types*)
        >>| fun x -> print x
  end
end

let () = MapReduce.register_app (module App)
