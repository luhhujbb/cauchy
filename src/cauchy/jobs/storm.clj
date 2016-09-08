(ns cauchy.jobs.storm
  (:require [clj-http.client :as http]
            [cheshire.core :refer :all]
            [clojure.string :as str]))


(defn fetch-cluster-metrics
  [host port]
  (let [url (str "http://" host ":" port "/api/v1/cluster/summary")]
    (try
      (http/get url {:throw-exceptions false :as :json})
      (catch Exception e
        {:status 500 :request-time -1}))))

(defn storm-cluster
  ([{:keys [host port]
     :or {host "127.0.0.1" port "8080"}}]
     (let [resp (fetch-cluster-metrics host port)]
      (if (= 200 (:status resp))
       (let [bd (:body resp)]
        [{:service "cluster.executorsTotal" :metric (:executorsTotal bd)}
         {:service "cluster.slotsFree" :metric (:slotsFree bd)}
         {:service "cluster.slotsTotal" :metric (:slotsTotal bd)}
         {:service "cluster.slotsUsed" :metric (:slotsUsed bd)}
         {:service "cluster.supervisors" :metric (:supervisors bd)}
         {:service "cluster.tasksTotal" :metric (:tasksTotal bd)}
         {:service "cluster.topologies" :metric (:topologies bd)}])
       {})))
  ([] (storm-cluster {})))

(defn topo-bolts
  [name bolts]
  (mapcat (fn [bolt]
    [{:service (str "topo." name ".bolt." (:boltId bolt) ".emitted") :metric (:emitted bolt)}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".acked") :metric (:acked bolt)}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".executed") :metric (:executed bolt)}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".executors") :metric (:executors bolt)}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".acked" ) :metric (:acked bolt)}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".failed") :metric (:failed bolt)}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".capacity") :metric (Float/parseFloat (:capacity bolt))}
     {:service (str "topo." name ".bolt." (:boltId bolt) ".executeLatency") :metric (Float/parseFloat (:executeLatency bolt))}])
    bolts))

(defn topo-spouts
  [name spouts]
  (mapcat (fn [spout]
    [{:service (str "topo." name ".spout." (:spoutId spout) ".emitted") :metric (:emitted spout)}
     {:service (str "topo." name ".spout." (:spoutId spout) ".acked") :metric (:acked spout)}
     {:service (str "topo." name ".spout." (:spoutId spout) ".transferred") :metric (:transferred spout)}
     {:service (str "topo." name ".spout." (:spoutId spout) ".executors") :metric (:executors spout)}
     {:service (str "topo." name ".spout." (:spoutId spout) ".acked" ) :metric (:acked spout)}
     {:service (str "topo." name ".spout." (:spoutId spout) ".failed") :metric (:failed spout)}
     {:service (str "topo." name ".spout." (:spoutId spout) ".capacity") :metric (Float/parseFloat (:completeLatency spout))}
     {:service (str "topo." name ".spout." (:spoutId spout) ".tasks") :metric (:tasks spout)}])
    spouts))

(defn s->u
  [string]
  (str/replace string #" " "_"))

(defn topo-stats
  [name stats]
  (mapcat
    (fn [stat]
      (let [stat-name (s->u (:windowPretty stat))
            metrics
                    [{:service (str "topo." name ".stat." stat-name ".acked" ) :metric (:acked stat)}
                     {:service (str "topo." name ".stat." stat-name ".emitted" ) :metric (:acked stat)}
                     {:service (str "topo." name ".stat." stat-name ".failed" ) :metric (:acked stat)}
                     {:service (str "topo." name ".stat." stat-name ".transferred" ) :metric (:transferred stat)}
                     {:service (str "topo." name ".stat." stat-name ".completeLatency" ) :metric (Float/parseFloat (:completeLatency stat))}]]
          (if-not (= stat-name "All_time")
            (conj metrics {:service (str "topo." name ".stat." stat-name ".acked_rate" ) :metric (float (/ (:acked stat) (Long/parseLong (:window stat))))})
            metrics)))
    stats))

(defn fetch-topology-details
  [host port topo-data]
  (let [url (str "http://" host ":" port "/api/v1/topology/" (:encodedId topo-data) "?sys=false")
        topo-data (:body (try
                              (http/get url {:throw-exceptions false :as :json})
                                (catch Exception e
                                  {:body nil})))]
      (if-not (nil? topo-data)
        (into []
          (concat
            (topo-bolts (:name topo-data) (:bolts topo-data))
            (topo-spouts (:name topo-data) (:spouts topo-data))
            (topo-stats (:name topo-data) (:topologyStats topo-data))
            [{:service (str "topo." (:name topo-data) ".workersTotal") :metric (:workersTotal topo-data)}
             {:service (str "topo." (:name topo-data) ".tasksTotal") :metric (:tasksTotal topo-data)}
             {:service (str "topo." (:name topo-data) ".executorsTotal") :metric (:executorsTotal topo-data)}
             {:service (str "topo." (:name topo-data) ".uptime") :metric (:uptimeSeconds topo-data)}]))
        [])))

(defn fetch-topologies
  [host port]
  (let [url (str "http://" host ":" port "/api/v1/topology/summary")
        topos-resp (try
                    (http/get url {:throw-exceptions false :as :json})
                      (catch Exception e
                        {:body {:topologies []}}))]
        (mapcat (partial fetch-topology-details host port) (get-in topos-resp [:body :topologies]))))

(defn storm-topologies
  ([{:keys [host port]
     :or {host "127.0.0.1" port "8080"}}]
     (fetch-topologies host port))
  ([] (storm-topologies {})))
