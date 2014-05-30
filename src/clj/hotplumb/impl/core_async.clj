(ns hotplumb.impl.core-async
  (:require [clojure.set :as set]
            [clojure.core.async :as async :refer [>! <! <!! >!! alts! close! chan go]]
            [hotplumb.impl.core-async.utils :as async-utils :refer [<? with-go? throw-if-err!]]
            [loom.graph :as graph]
            [loom.alg]
            [taoensso.timbre :as timbre]
            [hotplumb.core :as hotplumb :refer :all]))

(deftype OrderedSet [v s]
  clojure.lang.IPersistentCollection
  (seq [self]
    (seq v))
  (cons [self i]
    (if (contains? s i)
      self
      (OrderedSet. (conj v i) (conj s i))))
  (empty [self]
    (OrderedSet. [] #{}))
  (equiv [self o]
    (and (instance? OrderedSet o)
         (= v (.v o))))
  clojure.lang.ISeq
  (first [self] (first v))
  (next [self] (next v))
  (more [self] (rest v))
  Object
  (toString [self] (str \( "OrderedSet. " (pr-str v) " " (pr-str s) \))))

(defrecord ChangeRequest [resolved-tree remaining-atom done-chan]
  hotplumb/IChangeRequest
  (new-resolved-node [this key] (graph-node (:resolved-tree this) key))
  (ack-change-request! [this key]
    (timbre/debug "Processing ack for change request node" key)
    (let [remaining (swap! (:remaining-atom this) disj key)]
      (if (empty? remaining)
        (do
          (timbre/debug "No remaining keys for change request; triggering close mutex")
          (close! (:done-chan this)))
        (timbre/debug "Change request not yet complete; remaining nodes:" remaining))))
  (pending-change-request? [this key]
    (contains? @(:remaining-atom this) key))
  hotplumb/IDatum
  (datum-initial-value [this] (:change-type this))
  (datum-current-value [this] (:change-data this))
  (datum-path [this] [(:key this)])
  (datum-exception [this] nil)
  (datum-set-value [this new-value]
    (datum-set-value
     (->Datum (datum-initial-value this)
              (datum-current-value this)
              (datum-path this)
              (datum-exception this))
     new-value))
  (datum-append-path [this new-path]
    (datum-append-path
     (->Datum (datum-initial-value this)
              (datum-current-value this)
              (datum-path this)
              (datum-exception this))
     new-path)))

(defrecord RunningGraph [resolved-graph channel-map nodes]
  hotplumb/IRunningGraph
  (inject-value [g nk val]
    (let [c (get (:channel-map g) nk)
          datum (->Datum val val [] nil)]
      (if c
        (do
          (timbre/debug "Injecting value" val "into node" c "via datum" datum)
          (async/put! c datum))
        (throw (Exception. ""))))))

(defrecord RunningNode [state-atom close-chan in-chan
                        out-chans-atom closing-atom routine])

(defn callback-chan [f]
  "Invoke callback for side effects"
  (let [c (chan)]
    (go
      (loop [v (<! c)]
        (when-not (nil? v)
          (try
            (f v)
            (catch Exception ex
              (timbre/error ex "Ignoring callback failure")))
          (recur (<! c)))))
    c))

(defn write-to-children [node-key datum out-chans-atom]
  (with-go?
    (let [out-chans @out-chans-atom]
      (loop [pending-write-targets out-chans]
        (when-not (empty? pending-write-targets)
          (let [alt-list (vec (for [[out-chan _] pending-write-targets]
                                [out-chan datum]))
                _ (timbre/debug "Trying to perform write: " alt-list)
                [v c] (alts! alt-list)]
            (when-not v
              ;; indicates that output channel was closed
              (timbre/debug "Detected closed output channel writing from" node-key "to" (get out-chans c))
              (swap! out-chans-atom dissoc c))
            (recur (dissoc pending-write-targets c))))))))

(defn run-and-write [node-key resolved-node datum state state-atom out-chans-atom]
  (with-go?
    (let [processor (hotplumb/node-processor resolved-node)
          exception-handler (hotplumb/node-exception-handler resolved-node)
          is-change-request? (satisfies? IChangeRequest datum)]
      (when is-change-request?
        ;; write to children as-is BEFORE trying to run handler and build any requested children from same
        (<? (write-to-children node-key datum out-chans-atom)))
      (let [processing-result (try
                                (if is-change-request?
                                  (let [change-request-processor (hotplumb/node-change-handler resolved-node)]
                                    (if change-request-processor
                                      (hotplumb/process change-request-processor state datum)
                                      [state []]))
                                  (hotplumb/process processor state datum))
                                (catch Exception ex
                                  (let [datum (datum-set-exception datum ex)]
                                    (timbre/info "Handling exception" ex)
                                    (when exception-handler
                                      (hotplumb/process exception-handler state datum)))))]
        (let [[new-state new-values] processing-result]
          (if (nil? processing-result)
            nil ;; exit point: processor requested exit
            (do
              (when (not= state new-state)
                (reset! state-atom new-state))
              ;; write to outputs
              (doseq [new-value new-values]
                (<? (write-to-children node-key (datum-set-value datum new-value) out-chans-atom))))))))))

(defn run [initial-resolved-graph initial-channel-map node-key & {:keys [state-atom close-chan]}]
  (let [initial-resolved-node (hotplumb/graph-node initial-resolved-graph node-key)
        initial-processor (hotplumb/node-processor initial-resolved-node)
        in-chan (or (get initial-channel-map node-key)
                    (do
                      (timbre/error "Could not find" node-key "in channel-map" initial-channel-map)
                      (throw (Exception. "complete channel-map must exist before run is called"))))
        close-chan (or close-chan (chan))
        state-atom (or state-atom (atom (when initial-processor (hotplumb/initial-state initial-processor))))
        initial-out-chans (select-keys initial-channel-map (hotplumb/graph-sinks initial-resolved-graph node-key))
        initial-node-out-callback (:node-out-callback initial-resolved-node)
        out-chans-atom (atom (set/map-invert (if initial-node-out-callback
                                               (conj initial-out-chans [::callback (callback-chan initial-node-out-callback)])
                                               initial-out-chans)))
        closing-atom (atom false)
        initial-exception-handler (hotplumb/node-exception-handler initial-resolved-node)]
    (map->RunningNode
     {:state-atom state-atom
      :close-chan close-chan
      :in-chan in-chan ;; should never close unless node entirely removed from graph and remaining instance flushed / closed -- if replaced, replacement will read from same
      :out-chans-atom out-chans-atom ;; allow outside world to inspect our state, or preserve it on replacement
      :routine (with-go?
                 (try
                   (loop [resolved-node initial-resolved-node
                          state @state-atom]
                     (let [[v c] (alts! [in-chan close-chan] :priority true)]
                       (throw-if-err! v)
                       (timbre/debug "goroutine for" node-key "read" v "from channel" c)
                       (cond

                        (= c close-chan)
                        (do
                          (timbre/debug "Forcing immediate shutdown of goroutine for" node-key)
                          nil)

                        (= c in-chan)
                        (cond
                         (satisfies? hotplumb/IChangeRequest v)
                         (if (pending-change-request? v node-key)
                           (let [new-node (new-resolved-node v node-key)
                                 event-processor (and new-node
                                                      (satisfies? hotplumb/IDatum v)
                                                      (node-change-handler new-node))]
                             (timbre/debug "Processing an incoming change request for" node-key ":" v)
                             (timbre/debug "New resolved node:" new-node)
                             (ack-change-request! v node-key)
                             ;; for each child in the old topology,
                             ;; update set of output channels
                             (reset! out-chans-atom (set/map-invert (if initial-node-out-callback
                                                                      (conj initial-out-chans [::callback (callback-chan initial-node-out-callback)])
                                                                      initial-out-chans)))
                             ;; pass this through to our children
                             (if new-node
                               (recur new-node (<? (run-and-write node-key new-node v state state-atom out-chans-atom)))
                               (do
                                 (timbre/debug "Node" node-key "running change-request handler before shutdown")
                                 (<? (run-and-write node-key resolved-node v state state-atom out-chans-atom))
                                 nil)))
                           (recur resolved-node state))

                         (satisfies? hotplumb/IDatum v)
                         (when-not (nil? v) ;; exit point: in-chan closed externally
                           (let [datum (datum-append-path v node-key)]
                             (recur resolved-node (<? (run-and-write node-key resolved-node datum state state-atom out-chans-atom)))))))))
                   (catch Exception ex
                     (timbre/error ex "Unhandled exception in goroutine for" node-key))
                   (finally
                     (timbre/debug "Exiting goroutine for" node-key "and closing in-chan")
                     (close! in-chan))))})))

(defn build-channels-for
  "Unary use: Return a channel map; 2-arg use: update an existing rungraph with channels from a new resolved graph"
  ([resolved-graph]
     (into {}
           (for [[k v] (:active-nodes resolved-graph)]
             [k (chan)])))
  ([old-rungraph new-resolved-graph]
     (assoc old-rungraph :channel-map (build-channels-for new-resolved-graph))))

(defn add-nodes-to [running-graph new-nodes]
  (loop [graph running-graph
         pending-nodes new-nodes]
    (let [[node-key resolved-node] (first pending-nodes)
          remaining-nodes (rest pending-nodes)
          new-graph (assoc-in graph [:nodes node-key]
                              (run (:resolved-graph running-graph) (:channel-map running-graph) node-key))]
      (if remaining-nodes
        (recur new-graph remaining-nodes)
        new-graph))))

(defn add-new-nodes [running-graph new-resolved-graph node-keys]
  (let [old-resolved-graph (:resolved-graph running-graph)
        new-node-keys (set/difference (into #{} (keys (graph-nodes new-resolved-graph)))
                                      (into #{} (keys (graph-nodes old-resolved-graph))))]
    (loop [new-running-graph running-graph, node-keys new-node-keys]
      (let [node-key (first node-keys)]
        (if node-key
          (recur (assoc-in new-running-graph [:nodes node-key] (run new-resolved-graph (:channel-map running-graph) node-key))
                 (rest node-keys))
          new-running-graph)))))

;; Update algorithm
;; - Create new channels and nodes
;; - Pass change request through old topology, starting with any nodes having no sinks according to the OLD resolved graph.
(defn add-or-update-nodes [running-graph new-resolved-graph node-keys]
  (let [old-resolved-graph (:resolved-graph running-graph)
        done-chan (chan)
        change-request (->ChangeRequest new-resolved-graph (atom (into #{} (keys (graph-nodes old-resolved-graph)))) done-chan)
        old-entry-points (graph-entry-points old-resolved-graph)
        new-running-graph (add-new-nodes running-graph new-resolved-graph node-keys)]
    (with-go?
      (doseq [entry-point old-entry-points]
        (let [running-node (-> running-graph :nodes (get entry-point))
              in-chan (:in-chan running-node)]
          (>! in-chan change-request)))
      (<! done-chan) ;; running nodes were updated in-order;
      (assoc new-running-graph :resolved-graph new-resolved-graph))))

;; TODO: build changelists during simple (add/remove-node) edits?
(defn enact
  "Return a channel over which a running graph will be delivered when ready"
  ([new-resolved-graph existing-rungraph]
     (let [old-resolved-graph (:resolved-graph existing-rungraph)
           old-node-key-set (into #{} (keys (:active-nodes new-resolved-graph)))
           new-node-key-set (into #{} (keys (:active-nodes old-resolved-graph)))
           deleted-node-keys (clojure.set/difference old-node-key-set new-node-key-set)]
       (-> existing-rungraph
           (build-channels-for new-resolved-graph)
           (add-or-update-nodes new-resolved-graph (concat (active-nodes-tsort new-resolved-graph)
                                                           (filter deleted-node-keys
                                                                   (active-nodes-tsort old-resolved-graph)))))))

  ([resolved-graph]
     (let [new-channel-map (build-channels-for resolved-graph)]
       (with-go?
         (map->RunningGraph {:resolved-graph resolved-graph
                             :channel-map new-channel-map
                             :nodes (into {}
                                          (for [nk (active-nodes-tsort resolved-graph)
                                                :let [node (get (:active-nodes resolved-graph) nk)]]
                                            [nk (run resolved-graph new-channel-map nk)]))})))))
