(ns hotplumb.impl.core-async.utils
  (:require #+clj [clojure.core.async :as async :refer [alts! go <! >! chan close!]]
            #+clj [hotplumb.impl.core-async.utils.macros :as async-utils-macros]
            #+cljs [cljs.core.async :as async :refer [alts! <! >! chan close!]])
  #+cljs (:require-macros [cljs.core.async.macros :refer [go]]
                          [hotplumb.impl.core-async.utils.cljs-macros :refer [with-go? <?]]))

#+clj (defmacro with-go? [& args]
        `(async-utils-macros/with-go? ~@args))
#+clj (defmacro <? [& args]
        `(async-utils-macros/<? ~@args))

;; Thanks to David Nolen and Martin Trojer for throw-if-err! and <?

(defn throw-if-err! [e]
  "if e is an error, throw it; in all other cases, return"
  #+clj (when (instance? Throwable e) (throw e))
  #+cljs (when (instance? js/Error e) (throw e))
  e)

(defn map<?
  "A version of map< which passes exceptions within the returned channel"
  [f in-chan]
  (with-go? :dest out-chan
    (loop []
      (let [d (<? in-chan)]
        (when-not (nil? d)
          (let [v (f d)]
            (when (not (nil? v))
              (>! out-chan v))
            (recur)))))))

(defn split-chan
  "Given a channel containing [key value] pairs, yield a channel which, for each distinct key seen, yields [dest-key vals-chan]"
  [in-chan]
  (with-go? :dest out-chan
    (loop [channels {}]
      (let [item (<? in-chan)
            [item-key item-val] item
            item-chan (when item-key (channels item-key))]
        (cond
         (nil? item)
         (doseq [subchan-in (keys channels)]
           (close! subchan-in))

         (and item-chan (nil? item-val))
         (do
           (close! item-chan)
           (recur (dissoc channels item-key)))

         (and item-chan item-val)
         (do
           (>! item-chan item-val)
           (recur channels))

         (not (or item-chan item-val))
         (recur channels)

         (and item-val (not item-chan))
         (let [item-chan (chan)]
           (>! out-chan [item-key item-chan])
           (>! item-chan item-val)
           (recur (assoc channels item-key item-chan)))

         :else
         (recur channels))))))

(defn merge-channels
  "Given a channel which yields other channels, combine output from all of these into a single output channel

  Close when both the input channel and all goroutines read from that channel have closed."
  [in-chan]
  (with-go? :dest out-chan
    (loop [open-channels #{}, in-chan-open? true]
      (let [all-chans (if in-chan-open?
                        (conj open-channels in-chan)
                        open-channels)
            [v c] (when-not (empty? all-chans) (alts! (vec all-chans)))]
        (throw-if-err! v)
        (cond

         (empty? all-chans)
         nil

         (= c in-chan)
         (if v
           (recur (conj open-channels v) true)
           (recur open-channels false))

         (nil? v)
         (recur (disj open-channels c) in-chan-open?)

         :else
         (do
           (>! out-chan v)
           (recur open-channels in-chan-open?)))))))

(defn proxy-without-blocking-writes
  "Copy content from in-chan to out-chan

  If a write to out-chan would block, call (on-block-fn item-read in-chan out-chan)
  Continue if on-block-fn returns true; terminate if it returns false"
  ([in-chan out-chan on-block-fn]
     (proxy-without-blocking-writes in-chan out-chan on-block-fn true))

  ([in-chan out-chan on-block-fn close?]
     (with-go? :dest out-chan :preexisting? true :no-auto-close (false? close?)
       (loop []
         (let [in-item (<? in-chan)]
           (when-not (nil? in-item)
             (let [[v c] (alts! [[out-chan in-item]] :default :blocked)]
               (if (= c :default)
                 (when (on-block-fn in-item in-chan out-chan)
                   (recur))
                 (recur)))))))))

(defn proxy-without-blocking-writes<
  [in-chan & rest]
  (let [out-chan (chan)]
    (apply proxy-without-blocking-writes in-chan out-chan rest)
    out-chan))

(defn proxy-without-blocking-writes>
  [out-chan & rest]
  (let [in-chan (chan)]
    (apply proxy-without-blocking-writes in-chan out-chan rest)
    in-chan))
