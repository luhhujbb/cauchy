(ns cauchy.jobs.hadoop
  (:require [clj-http.client :as http]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]))

(defn fetch-metrics
  "Generic method to fetch jmx metrics"
  [{:keys [host port]}]
    (let [url (str "http://" host ":" port "/jmx")]
        (:body
          (try
            (http/get url {:as :json})
            (catch Exception e
              {:body {:error true}})))))

(defn filter-stats
  "retrieve a beans from a jmx json"
	[stats mygroup]
		(first (filter #(= (:name %) mygroup) (:beans stats))))


(defn namenode-cluster-info
  [input]
  [{:service "dfs.total" :metric (:Total input)}
   {:service "dfs.used" :metric (:Used input)}
   {:service "dfs.percentUsed" :metric (:PercentUsed input)}
   {:service "dfs.free" :metric (:Free input)}
   {:service "non-dfs.used" :metric (:NonDfsUsedSpace input)}
   {:service "nodes.live" :metric (count (json/parse-string (:LiveNodes input)))}
   {:service "nodes.dead" :metric (count (json/parse-string (:DeadNodes input)))}
   {:service "nodes.decom" :metric (count (json/parse-string (:DecomNodes input)))}])

(defn jvm-state
  [input]
  [{:service "mem.non_heap_max" :metric (:MemNonHeapMaxM input)}
   {:service "mem.non_heap_used" :metric (:MemNonHeapUsedM input)}
   {:service "mem.heap_max" :metric (:MemMaxM input)}
   {:service "mem.heap_used" :metric (:MemHeapUsedM input)}
   {:service "gc.count" :metric (:GcCount input)}
   {:service "gc.total_time" :metric (:GcTimeMillis input)}
   {:service "gc.avg_time" :metric (if-not (= 0 (:GcCount input)) (/ (:GcTimeMillis input) (:GcCount input)) 0)}])

 (defn namenode
   ([{:keys [host port period] :or {host "localhost" port "50070"} :as conf}]
   (let [metrics (fetch-metrics conf)]
      (if-not (:error metrics)
        (let [input-namenode (filter-stats metrics "Hadoop:service=NameNode,name=NameNodeInfo")
              input-jvm (filter-stats metrics "Hadoop:service=NameNode,name=JvmMetrics")]
              (into [] (concat (namenode-cluster-info input-namenode) (jvm-state input-jvm))))
        [])))
   ([] namenode {}))

(defn datanode
  ([{:keys [host port period] :or {host "localhost" port "50075"} :as conf}]
  (let [metrics (fetch-metrics conf)]
      (if-not (:error metrics)
        (let [input-jvm (filter-stats metrics "Hadoop:service=DataNode,name=JvmMetrics")]
              (into [] (jvm-state input-jvm)))
        [])))
  ([] datanode {}))
