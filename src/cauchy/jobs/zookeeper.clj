(ns cauchy.jobs.zookeeper
  (:require [clj-sockets.core :refer [create-socket write-to close-socket read-lines write-line]]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(def socket (atom nil))

(defn parse-mntr-line
  [line]
  (try
    (let [split-line (str/split line #"\t")
          key (get split-line 0)
          value (Long/parseLong  (get split-line 1))]
          {:service key :metric value})
    (catch Exception e
      nil)))

(defn get-zookeeper-metrics!
  [host port]
  (try
      ;;create socket
      (reset! socket (create-socket host port))
      ;;send monit command
      (write-to @socket "mntr")
      (let [data (read-lines @socket)
            ;;format to riemann metrcis
            data* (into [] (filter (fn [x] (not (nil? x))) (doall (map parse-mntr-line data))))]
        (close-socket @socket)
        ;;send metrics
        data*)
      (catch Exception e
        (log/error "[JOBS][ZOOKEEPER]" e))))

(defn zookeeper-health
  ([{:keys [host port] :as conf}]
    (get-zookeeper-metrics! host port))
  ([] zookeeper-health {}))
