open Async.Std
module P = Pipe

type 'a t = 'a P.Reader.t * 'a P.Writer.t

(*specs in *.mli for all three*)
let create () = P.create()

let push q x =
  match q with (r,w)-> Deferred.don't_wait_for(P.write w x)

let pop  q =
  match q with (r, w) -> let value = P.read r in 
  value>>= 
  (fun v -> match v with
  | `Eof -> failwith "queue shouldn't have been closed"
  | `Ok a -> return a)
