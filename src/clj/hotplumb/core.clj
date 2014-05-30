(ns hotplumb.core
  "Allow channel-connected flow graphs to be described in an immutable, deterministic manner, and modified (re-plumbed) live at runtime without losing data."
  (:require [clojure.set :as set]
            [clojure.core.async :as async]
            [loom.alg]
            [loom.graph :as graph]
            [loom.alg]
            [taoensso.timbre :as timbre]))

(defn ^:private set-deps [dep-graph key dep-list]
  "return a version of the given dependency graph with dependencies for the given key set to the given list"
  (timbre/debug "setting dependencies for" key "to" dep-list)
  (when (nil? key) (throw (Exception. "set-deps given nil key")))
  (graph/add-edges*
   (graph/remove-edges* dep-graph (graph/out-edges dep-graph key))
   (for [dep-key dep-list]
     (do
       (when (nil? dep-key) (throw (Exception. "set-deps given nil target key")))
       [key dep-key]))))

(defprotocol IDatum
  (datum-initial-value [this] "Initial value")
  (datum-current-value [this] "Current value, as modified")
  (datum-path [this] "Nodes traversed to reach current state")
  (datum-exception [this] "Exception, if any, which occurred during traversal")
  (datum-append-path [this path] "Return a version of this datum with an additional path element")
  (datum-set-value [this new-value] "Return a version of this datum with a different value set")
  (datum-set-exception [this new-exception] "Return a version of this datum with an exception attached"))

(defrecord Datum [input value path exception]
  IDatum
  (datum-initial-value [d] (:input d))
  (datum-current-value [d] (:value d))
  (datum-path [d] (:path d))
  (datum-exception [d] (:exception d))
  (datum-append-path [d new-path] (update-in d [:path] conj new-path))
  (datum-set-value [d new-value] (assoc d :value new-value))
  (datum-set-exception [d new-exception] (assoc d :exception new-exception)))

(defprotocol IChangeRequest
  (new-resolved-node [this key])
  (ack-change-request! [this key])
  (pending-change-request? [this key]))

(defprotocol IChannelProcessor
  (initial-state [this] "Initial state when starting the function")
  (process [this cur-state datum] "Return a new state and a sequence of outputs, potentially empty, to be emitted when new-value is seen -- or nil to shut down"))

(defprotocol IRunningGraph
  (inject-value [g nk val] "Inject an input value into the node addressed by key nk"))

(defprotocol IGraph
  (graph-node [g nk] "Return a node for the given key, if any exists")
  (graph-nodes [g] "Return a map of keys to IGraphNode instances")
  (graph-sources [g nk] "Given a graph, and a node key, return the list of keys whose output is written to this key"))

(defprotocol IGraphNode
  (node-processor [n] "an IChannelProcessor implementation, or nil for identity")
  (node-exception-handler [n] "an IChannelProcessor implementation for exceptions")
  (node-change-handler [n] "an IChannelProcessor implementation for topology changes")
  (node-in-keys [n] "List of keys for nodes which must be present as inputs")
  (node-out-callback [n] "If present, this function is called whenever an output value is available")
  (node-is-target? [n] "If true, this node is required to be present for a valid cluster; if false, it is not (unless depended on by a target)."))

(defprotocol IResolvedGraph
  (graph-entry-points [g] "Return keys for nodes with no sources")
  (graph-sinks [g nk] "Given a graph, and a node key, return the list of keys which read directly from this key")
  (node-is-active? [g nk] "True if this node is used by a target")
  (node-has-parents-outside-set [g nk nk-set] "True if the node with key nk is depended on by anything outside nk-set")
  (exclusive-dependant-nodes [g node-keys] "Return the given set of nodes, return a set extended with any exclusive downstreams (source-direction nodes)")
  (graph-upstream-nodes [g node-keys] "Given a set of node keys, determine the set of immediate upstreams (immediate sinks)")
  (active-nodes-tsort [g] "Return a topological sort of active nodes, ordered from sinks to sources"))

(defprotocol IEditableGraph
  (graph-add-nodes [g nodes] "Return a new graph with the given set of nodes added to the provided set")
  (graph-remove-nodes [g nodes] "Return a new graph with the given set of nodes removed from the provided set")
  (graph-set-constructor [g construction-func] "Update the graph as if it were built with the given constructor alone"))

;; default processor is identity
(extend-type nil
  IChannelProcessor
  (initial-state [this] nil)
  (process [this cur-state datum]
    [cur-state [(datum-current-value datum)]]))

;; stateless 1->1 processor
(defrecord SimpleFunctionProcessor [fn takes-datum?]
  IChannelProcessor
  (initial-state [this] nil)
  (process [this cur-state datum]
    (let [new-val ((:fn this) (if (takes-datum? this)
                                datum
                                (datum-current-value datum)))]
      (if new-val
        [cur-state [new-val]]
        [cur-state []]))))

;; stateful 1->1 processor
(defrecord StatefulFunctionProcessor [fn initial-state takes-datum?]
  IChannelProcessor
  (initial-state [this] (:initial-state this))
  (process [this cur-state datum]
    (let [[new-state new-val] ((:fn this) cur-state (if (:takes-datum? this)
                                                      datum
                                                      (datum-current-value datum)))]
      (if new-val
        [new-state [new-val]]
        [new-state []]))))

(def IdentityProcessor (->SimpleFunctionProcessor identity nil))

(defn assert-valid [resolved-graph]
  (when-not (loom.alg/dag? (:dep-graph resolved-graph))
    (throw (Exception. "Graph must not have cycles")))
  resolved-graph)

(defrecord GraphNode [processor exception-handler change-handler in-keys out-callback is-target?]
  IGraphNode
  (node-processor [n] (:processor n))
  (node-out-callback [n] (:out-callback n))
  (node-exception-handler [n] (:exception-handler n))
  (node-change-handler [n] (:change-handler n))
  (node-in-keys [n] (:in-keys n))
  (node-is-target? [n] (if (nil? (:is-target? n))
                         (not (nil? (:out-callback n)))
                         (:is-target? n))))

(declare resolved-graph)

(defn ^:private graph-check-deps
  "Given a potentially invalid graph and a list of nodes to start at, rebuild any invalid portions of the graph below this point"
  [g nodes]
  (loop [new-graph g
         pending-keys nodes
         done-keys #{}]
    (cond

     (not (seq pending-keys))
     (assert-valid new-graph)

     (contains? done-keys (first pending-keys))
     (recur new-graph (disj pending-keys (first pending-keys)) done-keys)

     :else
     (let [curr-key (first pending-keys)
           existing-node (get (:active-nodes new-graph) curr-key)]
       (if existing-node
         (do
           (recur new-graph
                  (set/difference (set/union (disj pending-keys curr-key)
                                             (node-in-keys existing-node))
                                  done-keys)
                  (conj done-keys curr-key)))
         (let [new-node
               (or
                (get (:provided-nodes new-graph) curr-key)
                (and (:node-factory new-graph)
                     ((:node-factory new-graph) curr-key)))
               new-node-deps (and new-node (node-in-keys new-node))]
           (when-not new-node
             (throw (Exception. (str "Unmet dependency on " (pr-str curr-key)))))
           (recur (assoc new-graph
                    :active-nodes (assoc (:active-nodes new-graph) curr-key new-node)
                    :dep-graph (set-deps (:dep-graph new-graph) curr-key new-node-deps))
                  (clojure.set/difference (clojure.set/union (disj pending-keys curr-key)
                                                             new-node-deps)
                                          done-keys)
                  (conj done-keys curr-key))))))))

(defrecord ResolvedGraph [active-nodes provided-nodes node-factory dep-graph]
  IGraph
  (graph-node [g nk] (or (get (:active-nodes g) nk)
                         (get (:provided-nodes g) nk)))
  (graph-nodes [g] (:active-nodes g))
  (graph-sources [g nk] (let [node (get (:active-nodes g) nk)]
                          (when node (:in-keys node))))

  IResolvedGraph
  (active-nodes-tsort [g] (loom.alg/topsort (:dep-graph g)))
  (graph-entry-points [g] (filter (fn [node-key] (empty? (graph-sources g node-key))) (active-nodes-tsort g)))
  (graph-sinks [g nk] (into #{} (map graph/src (graph/in-edges (:dep-graph g) nk))))
  (node-is-active? [g nk] (boolean (get (:active-nodes g) nk)))
  (node-has-parents-outside-set [g nk nk-set]
    (let [node-parents (graph-sinks g nk)]
      (not (clojure.set/subset? node-parents nk-set))))
  (exclusive-dependant-nodes [g nks]
    (loop [current-set nks]
      (let [all-child-keys (into #{} (for [k current-set
                                           source-key (graph-sources g k)]
                                       source-key))
            _ (timbre/debug "exclusive-dependant-nodes: all-children of" current-set
                            "is set" all-child-keys)
            filtered-keys (into #{} (for [child-key all-child-keys
                                          :when (not (or (node-is-target? (graph-node g child-key))
                                                         (node-has-parents-outside-set
                                                          g child-key current-set)))]
                                      child-key))
            _ (timbre/debug "exclusive-dependant-nodes: filtered-children of" current-set
                            "is set" filtered-keys)
            new-set (set/union current-set filtered-keys)]
        (if (= current-set new-set)
          current-set
          (recur new-set)))))
  (graph-upstream-nodes [g nks]
    (into #{} (for [k nks
                    sk (graph-sinks g k)]
                sk)))
  IEditableGraph
  (graph-remove-nodes [g nodes]
    "Remove downstream nodes not referenced exclusively by members of the given set and their exclusive children"
    (let [nodes-to-deactivate (exclusive-dependant-nodes g nodes)
          upstream-nodes (graph-upstream-nodes g nodes)]
      (-> g
          (assoc :provided-nodes (apply dissoc (:provided-nodes g) nodes)
                 :active-nodes (apply dissoc (:active-nodes g) nodes-to-deactivate)
                 :dep-graph (graph/remove-nodes* (:dep-graph g) nodes-to-deactivate))
          (graph-check-deps upstream-nodes))))
  (graph-add-nodes [g nodes]
    (let [new-targets (for [[k n] nodes :when (node-is-target? n)] k)
          replaced-keys (for [[k n] nodes :when (get (:active-nodes g) k)] k)
          replaced-key-parents (graph-upstream-nodes g replaced-keys)
          new-graph (assoc g
                      :provided-nodes (merge (:provided-nodes g) nodes)
                      :active-nodes (dissoc (:active-nodes g) replaced-keys))]
      (graph-check-deps new-graph (into #{} (concat new-targets replaced-key-parents)))))
  (graph-set-constructor [g construction-func]
    (let [new-active-nodes (into {} (for [[k v] (:provided-nodes g)
                                          :when (node-is-target? v)]
                                      [k v]))]
      (resolved-graph (:provided-nodes g) construction-func))))

(def EMPTY-GRAPH (->ResolvedGraph {} {} nil (graph/digraph)))

(defn resolved-graph
  "Given a dictionary of nodes, and potentially a factory to use in constructing missing ones, return a resolved graph"
  ([nodes]
     (resolved-graph nodes nil))
  ([nodes node-factory]
     (assert-valid (graph-add-nodes (assoc EMPTY-GRAPH :node-factory node-factory) nodes))))
