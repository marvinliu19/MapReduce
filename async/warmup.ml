open Async.Std

(** 
* fork takes a deferred computation and two functions, and runs 
* the two functions concurrently when the deferred computation 
* becomes determined, passing those functions the value determined 
* by the original deferred 
**)
let fork d f1 f2 =
  ignore(Deferred.both (d >>= f1) (d >>= f2))

(** deferred_map should has same input–output behavior as List.map. 
* It takes a list l, a function f, and return the result of mapping 
* the list through the function. But deferred_map should apply f 
* concurrently—not sequentially—to each element of l. **)
let deferred_map (l: 'a list) (f: 'a -> 'b Deferred.t) : 'b list Deferred.t =
  List.fold_left 
    (fun acc x ->
      (f x) >>= (fun h ->
      acc >>= (fun t -> 
      return (t@[h]))))
    (return []) l
