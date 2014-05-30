(ns hotplumb.impl.core-async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [<!!]]
            [hotplumb.core :as hotplumb]
            [hotplumb.impl.core-async :as async-impl]))

(when *load-tests*

  (def recording-processor
    (hotplumb/->StatefulFunctionProcessor
     (fn [cur-state datum]
       (let [value (:value datum)]
         [(conj cur-state datum) value]))
     [] true))

  (def trivial-runnable-topology
    (hotplumb/resolved-graph
     {:sink (hotplumb/map->GraphNode {:processor recording-processor
                                      :in-keys #{:inter}
                                      :is-target? true})
      :inter (hotplumb/map->GraphNode {:in-keys #{:source}})
      :source (hotplumb/map->GraphNode {})}))

  (def trivial-runnable-topology-replace-inter
    (hotplumb/resolved-graph
     {:sink  (hotplumb/map->GraphNode {:processor recording-processor
                                      :in-keys #{:inter2}
                                      :is-target? true})
      :inter2 (hotplumb/map->GraphNode {:in-keys #{:source}})
      :source (hotplumb/map->GraphNode {})})))

(deftest trivial-test
  (testing "Ability to pass content through a (very, very) simple topology"
    (is (= {:value "Testing!"
            :path [:source :inter :sink]
            :exception nil}
           (let [running-tree (<!! (async-impl/enact trivial-runnable-topology))]
             (hotplumb/inject-value running-tree :source "Testing!")
             (async/<!! (async/timeout 500))
             (-> running-tree :nodes :sink :state-atom deref last
                 (select-keys [:value :path :exception]))))
        (= [{:value "Testing!"
             :path [:source :inter :sink]
             :exception nil}
            {:value "Testing Again!"
             :path [:source :inter2 :sink]
             :exception nil}]
           (let [running-tree (<!! (async-impl/enact trivial-runnable-topology))]
             (hotplumb/inject-value running-tree :source "Testing!")
             (async/<!! (async/timeout 500))
             (let [running-tree (<!! (async-impl/enact trivial-runnable-topology-replace-inter running-tree))]
               (hotplumb/inject-value running-tree :source "Testing Again!")
               (-> running-tree :nodes :sink :state-atom deref vec
                   (select-keys [:value :path :exception]))))))))
