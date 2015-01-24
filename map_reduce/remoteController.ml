open Async.Std
open Protocol
module AQ = AQueue

(*maintain a reference to the address list*)
let addr_list = ref [("", 0)]

(*queue of workers - empty before controller initialized and after map_reduce
  finished (workers have been disconnected)*)
let workers = (AQ.create ())

let init (addrs: (string * int) list): unit =
  addr_list := addrs

exception InfrastructureFailure
exception MapFailure of string
exception ReduceFailure of string

module Make (Job : MapReduce.Job) = struct

  module WReq = WorkerRequest(Job)
  module WRes = WorkerResponse(Job)

  (*keep track of the connection count*)
  let connection_count = ref 0
  
  (* Establishes connections to the given address. Creates a worker if the
   * connection is successful and adds it to the queue of workers. 
   * Raise InfrastructureFailure if there no connections. 
   * @param: addrs - a list of hosts and ports to connect to *)
  let init_worker (addr: string * int): unit Deferred.t = 
    match addr with
      | (host, port) -> try_with(fun () -> 
          Tcp.connect (Tcp.to_host_and_port host port)) >>| function
            | Core.Std.Ok (s,r,w) -> ( Writer.write_line w Job.name; 
                            connection_count := !connection_count + 1;
                            AQ.push workers (r,w))
            | Core.Std.Error _ -> ()

  (**
    * connects all the workers in address list
    **)
  let connect_workers (adrlst: (string*int) list): unit list Deferred.t= 
    Deferred.List.map ~how:`Parallel adrlst ~f:init_worker

  (* Assigns a single input to a worker and returns the result (key, inter) list
   * @param: input - a single input to be given to the worker
   * @return: the (key, inter) list result from mapping the input *)            
  let map (input: Job.input) : (Job.key * Job.inter) list Deferred.t =
    (AQ.pop workers) >>= fun x -> (let (r, w) = x in
    WReq.send w (WReq.MapRequest input);
    (WRes.receive r) >>= function
      | `Ok (WRes.MapResult result) -> AQ.push workers x; return result
      | `Ok (WRes.JobFailed error) -> raise (MapFailure error)
      | `Ok (WRes.ReduceResult out) -> 
          raise (MapFailure "ReduceResult received when MapResult expected.")
      | `Eof -> raise (MapFailure "Connection to worker closed during the map phase"))

  (* Distributes each input to the workers and returns a list of (key, inter)
   * lists.
   * @param: ins - a list of inputs to be mapped
   * @return: a list containing the (key,inter) lists from each input *)
  let map_phase (ins: Job.input list): (Job.key*Job.inter) list list Deferred.t=
    Deferred.List.map ~how:`Parallel ins ~f:map
  
  module C = Combiner.Make(Job)

  (**
    * combine_phase takes the accumulated results of list and uses the combine
    * module to group the key*inter list into elements of one key, and then a 
    * list of inters
  **)
  let combine_phase (inters: (Job.key*Job.inter) list list): 
    (Job.key * Job.inter list) list Deferred.t=
    return (C.combine (List.flatten inters))

  (*helper function that processes a reduce request, and receives an output
  Note: the output corresponds to the same request *)
  let reduce ((key: Job.key), (vs: Job.inter list)):
     (Job.key*Job.output) Deferred.t= 
    (*get a worker*)
    (AQ.pop workers)>>= (fun x -> let (r,w) = x in   
    WReq.send w (WReq.ReduceRequest (key,vs) ); 
    (WRes.receive r)>>|
    (fun e -> match e with 
        `Ok (WRes.JobFailed s) -> raise (ReduceFailure s)
      | `Ok (WRes.ReduceResult output) -> AQ.push workers x; (key, output)
      | `Ok (WRes.MapResult output) -> 
        raise (ReduceFailure "incorrect type of result (Map when Reduce expected)")
      | `Eof -> raise (ReduceFailure "server connection lost" )))

  (*This function handles the entire reduce_phase, sending a single reduce job
   * to a worker server*)
  let reduce_phase (inters: (Job.key* Job.inter list) list):
    ((Job.key*Job.output) list ) Deferred.t = 
    (Deferred.List.map ~how: `Parallel inters ~f: reduce) 
  
  (*see .mli for framework Controller.map_reduce specification*)
  let map_reduce (inputs: Job.input list): (Job.key*Job.output) list Deferred.t=
    let result = (connect_workers !addr_list) >>= (fun _ ->
      if !connection_count=0 then raise InfrastructureFailure else
      (map_phase inputs) >>= (fun x ->
      (combine_phase x) >>= (fun y -> reduce_phase y))) in

    (for i = !connection_count downto 0 do 
      Deferred.don't_wait_for((AQ.pop workers)>>= (fun (r,w) -> Writer.close w));
    done);

    result

end

