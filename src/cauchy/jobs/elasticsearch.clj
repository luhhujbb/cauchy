(ns cauchy.jobs.elasticsearch
  (:require [clj-http.client :as http]))

(def hostname (.. java.net.InetAddress getLocalHost getHostName))

(defn fetch-health
  [{:keys [host port] :or {host hostname port 9200}}]
  (let [url (str "http://" host ":" port "/_cluster/health")]
    (:body (http/get url {:as :json}))))

(defn fetch-stats
  [{:keys [host port] :or {host hostname port 9200}}]
  (let [url (str "http://" host ":" port "/_stats?fielddata=true")]
    (:body (http/get url {:as :json}))))

(defn fetch-local
  [{:keys [host port] :or {host hostname port 9200}}]
  (let [url (str "http://" host ":" port "/_nodes/_local/stats")]
    (:body (http/get url {:as :json}))))

(defn fetch-recovery
  [{:keys [host port] :or {host hostname port 9200}}]
  (let [url (str "http://" host ":" port "/_recovery")]
    (:body (http/get url {:as :json}))))

(defn get_story []
    (http/post "http://localhost:9200/_search"
    {:body "{\"query\": {\"range\": {\"created\": {\"gt\": \"now-1m\" }}}}" :as :json}))

(defn get-node-id
  [conf]
  (let [nodes-id (->> (fetch-local conf)
                      (:nodes)
                      (keys))]
    (when (= 1 (count nodes-id))
      (name (first nodes-id)))))

(defn getstoryhits
    ([{:keys [warn ok] :as conf :or {ok 100 warn 30}}]
      (let [{{:keys [total]} :hits} ((get_story) :body) ]
        (cond
         (>= total ok ) (def status "ok")
         (>= total warn) (def status "warning")
          :else (def status "critical"))
       [{ :service (str "hits") :metric total :state status }])))

(defn count-local-active-shards
  [conf]
  (when-let [node-id (get-node-id conf)]
    (let [status (fetch-recovery conf)
          shard-infos (for [index (keys status)
                            shard-num (keys (get-in status [index :shards]))]
                        (get-in status [index :shards shard-num]))]

      (reduce (fn [acc info]
                (if (= node-id (get-in info [:target :id]))
                  (inc acc)
                  acc))
              0
              (flatten shard-infos)))))

(def color->state
  {"green" "ok"
   "yellow" "warning"
   "red" "critical"})

(defn elasticsearch-health
  ([{:keys [host port] :as conf}]
   (let [{:keys [status active_shards
                 unassigned_shards relocating_shards
                 initializing_shards active_primary_shards
                 number_of_nodes number_of_data_nodes]

          :as health} (fetch-health conf)
         stats (fetch-stats conf)]
     [
      ;;Cluster metrics
      {:service "color" :state (color->state status)}
      {:service "local_active_shards"
       :metric (count-local-active-shards conf)}
      {:service "docs_in_cluster"
       :metric (get-in stats [:_all :primaries :docs :count])}
      {:service "docs_deleted_count"
       :metric (get-in stats [:_all :primaries :docs :deleted])}
      {:service "cluster_size_in_bytes"
       :metric (get-in stats [:_all :primaries :store :size_in_bytes])}
      {:service "fielddata_memory_size_in_bytes"
       :metric (get-in stats [:_all :total :fielddata :memory_size_in_bytes])}
      {:service "fielddata_evictions"
       :metric (get-in stats [:_all :total :fielddata :evictions])}
      {:service "active_shards" :metric active_shards}
      {:service "unassigned_shards" :metric unassigned_shards}
      {:service "relocating_shards" :metric relocating_shards}
      {:service "initializing_shards" :metric initializing_shards}
      {:service "active_primary_shards" :metric active_primary_shards}
      {:service "number_of_nodes" :metric number_of_nodes}
      {:service "number_of_data_nodes" :metric number_of_data_nodes}
      ]))
  ([] (elasticsearch-health {})))

(defn elasticsearch-node-stats
    ([{:keys [host port] :as conf}]
      (let [ [node-id node-stats] (first (get-in (fetch-local conf) [:nodes]))]
        (vec (concat [
        ;;Node indices metrics
        {:service "indices.indexing.total" :metric (get-in node-stats [:indices :indexing :index_total])}
        {:service "indices.indexing.current" :metric (get-in node-stats [:indices :indexing :index_current])}
        {:service "indices.get.total" :metric (get-in node-stats [:indices :get :total])}
        {:service "indices.get.current" :metric (get-in node-stats [:indices :get :current])}
        {:service "indices.search.query_total" :metric (get-in node-stats [:indices :search :query_total])}
        {:service "indices.search.query_current" :metric (get-in node-stats [:indices :search :query_current])}
        {:service "indices.search.open_contexts" :metric (get-in node-stats [:indices :search :open_contexts])}
        ;;Node jvm heap metrics
        {:service "jvm.heap.used_bytes" :metric (get-in node-stats [:jvm :mem :heap_used_in_bytes])}
        {:service "jvm.heap.used_pct" :metric (get-in node-stats [:jvm :mem :heap_used_percent])}
        {:service "jvm.heap.max_bytes" :metric (get-in node-stats [:jvm :mem :heap_max_in_bytes])}
        {:service "jvm.non_heap.used_bytes" :metric (get-in node-stats [:jvm :mem :non_heap_used_in_bytes])}
        ;;Node jvm gc metrics
        {:service "jvm.gc.young.count" :metric (get-in node-stats [:jvm :gc :collectors :young :collection_count])}
        {:service "jvm.gc.young.collection_time_in_millis" :metric (get-in node-stats [:jvm :gc :collectors :young :collection_time_in_millis])}
        {:service "jvm.gc.old.count" :metric (get-in node-stats [:jvm :gc :collectors :old :collection_count])}
        {:service "jvm.gc.old.collection_time_in_millis" :metric (get-in node-stats [:jvm :gc :collectors :old :collection_time_in_millis])}
        ]
        ;;Thread data
        (mapcat
          (fn [[thread_type thread_data]]
            [{ :service (str "thread_pool." (name thread_type) ".nb") :metric (get-in thread_data [:threads]) }
            { :service (str "thread_pool." (name thread_type) ".queue") :metric (get-in thread_data [:queue]) }
            { :service (str "thread_pool." (name thread_type) ".completed") :metric (get-in thread_data [:completed]) }
            { :service (str "thread_pool." (name thread_type) ".largest") :metric (get-in thread_data [:largest]) }
            ]
          ) (get-in node-stats [:thread_pool])
        )
        ))
      ))
  ([] (elasticsearch-node-stats {}))
)
