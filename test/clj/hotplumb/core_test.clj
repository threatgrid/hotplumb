(ns hotplumb.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [go <! <!! >! chan close!]]
            [hotplumb.core :refer :all]
            [taoensso.timbre :as timbre]))

(when *load-tests*

  (defn test-factory [key-in]
    (if (and (vector? key-in)
             (= (first key-in) :linear))
      (let [cur-val (second key-in)
            next-val (dec cur-val)]
        (if (> next-val 0)
          (map->GraphNode {:in-keys #{[:linear next-val]}})
          (map->GraphNode {:in-keys #{:source}})))
      (throw (Exception. (str "Unable to build node for " (pr-str key-in))))))

  (def test-graph
    (resolved-graph
     {:source (map->GraphNode {:in-chan ::assert-input})
      :sink (map->GraphNode {:in-keys #{[:linear 3]} :is-target? true})
      :node-unused (map->GraphNode {:in-keys #{[:linear 5]}})
      :source-unused (map->GraphNode {:in-chan ::another-input-exists})}
     test-factory))

  (def test-multiple-graphs
    (graph-add-nodes
     test-graph
     {:other-sink (map->GraphNode {:in-keys #{:other-source} :is-target? true})
      :other-source (map->GraphNode {})})))

(deftest basic-test
  (testing "simple graph protocol"
    (is (= #{[:linear 3]} (graph-sinks test-graph [:linear 2])))
    (is (= #{[:linear 1]} (graph-sources test-graph [:linear 2])))
    (is (= true (node-is-active? test-graph [:linear 2])))
    (is (= false (node-is-active? test-graph [:linear 4])))

    ;; adding new nodes with dependencies adds those dependencies
    ;; such nodes can be explicit targets, even with no in-sink
    (is (= true
           (node-is-active?
            (graph-add-nodes test-graph {:new-sink (map->GraphNode
                                                    {:in-keys #{[:linear 6]}
                                                     :is-target? true})})
            [:linear 5])))

    ;; adding new nodes with dependencies adds those dependencies
    ;; such nodes can be implicit targets, via an out-chan
    (is (= true
           (node-is-active?
            (graph-add-nodes test-graph {:new-sink (map->GraphNode
                                                    {:in-keys #{[:linear 6]}
                                                     :out-callback ::makes-me-important})})
            [:linear 5])))

    ;; adding new nodes which are not important has no immediate effect
    (is (= false
           (node-is-active?
            (graph-add-nodes test-graph {:new-sink (map->GraphNode
                                                    {:in-keys #{[:linear 6]}})})
            [:linear 5])))

    ;; dependency tree runs from sinks to sources
    (is (= #{:sink [:linear 3] [:linear 2] [:linear 1] :source}
           (exclusive-dependant-nodes test-graph #{:sink})))
    (is (= #{} (graph-upstream-nodes test-graph #{:sink})))))

;; TODO: Add some tricky ones:
;; - Replace existing nodes w/ new ones with fewer dependencies
;; - Remove a manually provided node and ensure that a generated one replaces it

(when *load-tests*
  (defn factor-base2 [n]
    (loop [cur-val n
           result #{}
           cur-fact 1]
      (if (> cur-val 0)
        (if (not= 0 (bit-and cur-val 1))
          (recur (bit-shift-right cur-val 1)
                 (conj result cur-fact)
                 (* cur-fact 2))
          (recur (bit-shift-right cur-val 1)
                 result
                 (* cur-fact 2)))
        result)))

  (defn branching-test-factory [key-in]
    (cond

     (and (vector? key-in)
          (= (first key-in) :filter))
     (let [cur-val (second key-in)]
       (map->GraphNode {:in-keys (into #{}
                                       (for [n (factor-base2 cur-val)]
                                         [:source n]))}))

     :else nil))

  (defn branching-test-factory-2 [key-in]
    (if (= key-in :extra-source)
      (map->GraphNode {:in-chan ::assert-fanout-input})
      (let [orig-out (branching-test-factory key-in)]
        (when-not (nil? orig-out)
          (assoc orig-out
            :in-keys (conj (:in-keys orig-out) :extra-source))))))

  (def branching-graph-sources
    {[:source 1] (map->GraphNode {:in-chan ::assert-input-1})
     [:source 2] (map->GraphNode {:in-chan ::assert-input-2})
     [:source 4] (map->GraphNode {:in-chan ::assert-input-4})
     [:source 8] (map->GraphNode {:in-chan ::assert-input-8})})

  (def branching-graph-client-set
    {[:client 1] (map->GraphNode {:in-keys #{[:filter 4]}
                                  :is-target? true})
     [:client 2] (map->GraphNode {:in-keys #{[:filter 5]}
                                  :is-target? true})
     [:client 3] (map->GraphNode {:in-keys #{[:filter 6]}
                                  :is-target? true})})

  (def branching-graph-base
    (resolved-graph branching-graph-sources branching-test-factory))

  (def branching-graph-with-clients
    (resolved-graph (into {} (concat
                              branching-graph-sources
                              branching-graph-client-set))
                    branching-test-factory)))

(deftest branching-test
  (testing "cleanly-constructed graphs and edited graphs should be equal if they have matching construction functions and user-provided nodes"
    (is (= (graph-add-nodes branching-graph-base branching-graph-client-set)
           branching-graph-with-clients))
    (is (= branching-graph-base
           (graph-remove-nodes branching-graph-with-clients
                               (into #{} (keys branching-graph-client-set)))))
    (is (= (graph-add-nodes branching-graph-base
                            (dissoc branching-graph-client-set [:client 1]))
           (graph-remove-nodes branching-graph-with-clients #{[:client 1]})))
    (is (= (graph-add-nodes branching-graph-base
                            (dissoc branching-graph-client-set [:client 3]))
           (graph-remove-nodes branching-graph-with-clients #{[:client 3]})))
    (is (= (graph-set-constructor branching-graph-with-clients branching-test-factory-2)
           (resolved-graph (into {} (concat
                              branching-graph-sources
                              branching-graph-client-set))
                    branching-test-factory-2)))))
