(ns cauchy.jobs.ping
  (:require [clojure.string :as s])
  (:use [clojure.java.shell :only [sh]]))

(defn execute-ping
    [host]
    (let [raw-res (sh "/bin/ping" "-c" "5" host)]
        (when (= 0 (:exit raw-res))
         (s/split-lines (:out raw-res)))))

(defn parse-ping-latency
    [res]
    (let [splitted-res (s/split res #" ")
          metric-names (s/split (get splitted-res 1) #"/")
          metric-values (map (fn [^String x] (Double/parseDouble x)) (s/split (get splitted-res 3) #"/"))]
          (zipmap metric-names metric-values)))

(defn parse-ping-loss
    [res]
    (let [loss-matcher (re-find (re-pattern "^.* ([0-9]{1,3})\\%.*$") res)]
        {:service "ping.loss" :metric (last loss-matcher)}))

(defn ping
    ([{:keys [host]
       :or {host "8.8.8.8"}}]
       (let [res (execute-ping host)
             latency-raw (last res)
             loss-raw (last (butlast res))]
      (vec
          (concat
              (map
                  (fn [[k v]] {:service (str "ping." k) :metric v})
                  (parse-ping-latency latency-raw))
               [(parse-ping-loss loss-raw)]))))
     ([] (ping {})))
