open Async.Std
open Protocol

module Make (Job : MapReduce.Job) = struct
  module WReq = WorkerRequest(Job)
  module WRes = WorkerResponse(Job)
  (* see .mli *)
  let rec run (r: Reader.t) (w: Writer.t): unit Deferred.t =
    (WReq.receive r) >>= function
      | `Eof -> return ()
      | `Ok (WReq.MapRequest input) ->  
        (try_with(fun () -> (Job.map input)) >>= function
          | Core.Std.Ok x-> 
              WRes.send w (WRes.MapResult x);
              run r w
          | Core.Std.Error e -> 
              WRes.send w (WRes.JobFailed (Printexc.to_string e));
              run r w)
      | `Ok (WReq.ReduceRequest (key, inters)) ->  
        (try_with(fun () -> (Job.reduce (key,inters))) >>= function
          | Core.Std.Ok x-> 
              WRes.send w (WRes.ReduceResult x);
              run r w
          | Core.Std.Error e -> 
              WRes.send w (WRes.JobFailed (Printexc.to_string e));
              run r w)
end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


