open Async.Std
open Async_unix

type filename = string

(******************************************************************************)
(** {2 The Inverted Index Job}                                                *)
(******************************************************************************)

module Job = struct
  type input = (filename * string) (*the string is all contents of the file*)
  type key = string
  type inter = filename
  type output = filename list

  let name = "index.job"

  (*map takes a filename string pair, and gives a list of pairs, with the first element
    the word, and the second list is the filename itself *)
  let map input : (key * inter) list Deferred.t =
    (return input)>>| (fun input ->
    let (fname, contents) = input in
    let keys = AppUtils.split_words contents in
    List.fold_left (fun acc x -> (x, fname)::acc) [] keys)

  (* reduce takes a key which is the word, and combines the inters list, which is a 
    list of filenames (makes sure not to repeat filenames in the list)*)
  let reduce (key, inters) : output Deferred.t =
    (return inters)>>| (fun lst -> 
      List.fold_left (fun acc x -> 
        if (List.mem x acc) then acc else x::acc) [] lst )
end

(* register the job *)
let () = MapReduce.register_job (module Job)


(******************************************************************************)
(** {2 The Inverted Index App}                                                *)
(******************************************************************************)

module App  = struct

  let name = "index"

  (** Print out all of the documents associated with each word *)
  let output results =
    let print (word, documents) =
      print_endline (word^":");
      List.iter (fun doc -> print_endline ("    "^doc)) documents
    in

    let sorted = List.sort compare results in
    List.iter print sorted


  (** for each line f in the master list, output a pair containing the filename
      f and the contents of the file named by f.  *)
  let read (master_file : filename) : (filename * string) list Deferred.t =
    Reader.file_lines master_file >>= fun filenames ->

    Deferred.List.map filenames (fun filename ->
      Reader.file_contents filename >>| fun contents ->
      (filename, contents)
    )

  module Make (Controller : MapReduce.Controller) = struct
    module MR = Controller(Job)

    (** The input should be a single file name.  The named file should contain
        a list of files to index. *)
    let main args =
      match args with 
      x::[] ->
        (read x) (*helper function gives us list of inputs*)
          >>= MR.map_reduce
          >>| output 
      | [] -> failwith "exactly one filename expected"
      | h::t -> failwith "exactly one filename expected"
  end
end

(* register the App *)
let () = MapReduce.register_app (module App)

