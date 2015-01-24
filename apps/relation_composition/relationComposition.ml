open Async.Std
open AppUtils

type relation = R | S

module Job = struct
  type input  = relation * string * string
  type key    = string
  type inter  = relation * string
  type output = (string * string) list

  let name = "composition.job"

  (* Takes a (relation, x y) input and outputs [(key,inter)]
   * If the relation is R, the key is y, and the inter is (R, x)
   * If the relation is S, the key is x, and the inter is (S, y)
   * The key is the term y in Y that the two relations R and S have in common
   * @param: an input of relation*string*string
   * @return: a (key*inter) list deferred *)
  let map ((r, x, y):input) : (key * inter) list Deferred.t =
    match r with
      | R -> return [(y, (R, x))]
      | S -> return [(x, (S, y))]

  (* Takes a key and a list of inters and returns a list of outputs
   * Composes all the R relations in the list with all the S relations
   * @param: an input of key * inter list
   * @return: a output deferred *)
  let reduce (k, vs): output Deferred.t=
    let rec matcher (acc: output) (x: string) (inters: inter list): output =
      match inters with
      | [] -> acc
      | (R, _ )::t -> matcher acc x t
      | (S, y)::t -> matcher ((x,y)::acc) x t
    in
    let rec helper (acc: output) (inters: inter list): output = 
      match inters with
      | [] -> acc
      | (R, x)::t -> helper (acc@(matcher [] x vs)) t
      | (S, _ )::t -> helper acc t
    in return (helper [] vs)
end

let () = MapReduce.register_job (module Job)

let read_line (line: string) : (string * string) =
  match Str.split (Str.regexp ",") line with
    | [domain; range] -> (String.trim domain, String.trim range)
    | _ -> failwith "Malformed input in relation file."

let read_file (r: relation) (file: string) : (relation * string * string) list Deferred.t =
  Reader.file_lines file            >>= fun lines  ->
  return (List.map read_line lines) >>= fun result ->
  return (List.map (fun (domain, range) -> (r, domain, range)) result)

module App = struct
  let name = "composition"

  let clean_and_print vs =
    List.map snd vs   |>
    List.flatten      |>
    List.sort compare |>
    List.iter (fun (a, b) -> printf "(%s, %s)\n" a b)

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (* You may assume that main is called with two valid relation files. You do
       not need to handle malformed input. For example relation files, see the
       data directory. *)
    let main args =
      match args with
      | [rfile; sfile] -> begin
          read_file R rfile >>= fun r ->
          read_file S sfile >>= fun s ->
          return (r @ s)
          >>= MR.map_reduce
          >>| clean_and_print
      end
      | _ -> failwith "Incorrect number of input files. Please provide two files."
  end
end

let () = MapReduce.register_app (module App)
