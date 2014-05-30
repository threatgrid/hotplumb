(ns hotplumb.impl.core-async.utils.macros
  (:require [clojure.core.async :as async :refer [alts! go <! >! chan close!]]
            [taoensso.timbre :as timbre]))

;; thanks to David Nolen for <?
(defmacro <? [ch]
  "non-blocking read from ch, throwing the result should it be an error"
  `(hotplumb.impl.core-async.utils/throw-if-err! (<! ~ch)))

(defn ^:private find-leading-options
  "Return a map of k/v pairs and a sequence of remaining content"
  [opts-and-body]
  (let [opts (into {} (map vec (take-while (fn [[k v]] (keyword? k))
                                           (partition 2 opts-and-body))))
        opts-len (* 2 (count opts))
        body (nthrest opts-and-body opts-len)]
    [opts body]))

(defmacro with-go? [& opts-and-body]
  "A go block with an optional explicit destination channel

  Keyword options -
    :dest <name> - channel which exceptions will be routed to.
    :preexisting? <bool> - if true, create a channel by given dest name.
                           if false, this channel must exist in calling context.
    :no-auto-close <bool> - unless true, destination channel will be closed on exit.

  Thrown exceptions will be passed into either the destination channel or the return value"
  (let [[options body] (find-leading-options opts-and-body)
        {:keys [dest preexisting? no-auto-close]} options
        ex-sym (gensym "ex__")]
    `(~@(if (and dest (not preexisting?))
          `(let [~dest (chan)])
          `(when true))
      (go
        (try
          ~@body
          (catch Throwable ~ex-sym
            (timbre/trace ~ex-sym "Passing exception to result channel for handling")
            ~(if dest
               `(>! ~dest ~ex-sym)
               ex-sym))
          ~@(when dest
              `((finally
                  ~(if (not no-auto-close)
                     `(do
                        (timbre/trace "closing channel on completion")
                        (close! ~dest))
                     `(timbre/trace "skipping auto-close")))))))
      ~@(when dest
          `(~dest)))))
