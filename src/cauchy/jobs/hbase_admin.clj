(ns cauchy.jobs.hbase-admin
  (:require [hbase.admin.core :as hbase]
            [clojure.string :as str]))

;;atom to store if habse connection is enabled
(def init? (atom {}))

(defn get-admin
  [hb]
  (hbase/get-admin (hbase/get-connection hb)))

(defn fetch-cluster-basics
  [hb]
  (let [cluster-status (hbase/get-cluster-status (get-admin hb))]
    (into []
      ;;map on metrics
      (map
      (fn [[k v]]
        {:service (name k) :metric v})
        (select-keys
          cluster-status
          [:nb-dead-servers
           :nb-master-backup
           :nb-live-servers
           :nb-requests
           :nb-regions])))))

(defn fetch-regionservers-load
  [hb]
  (let [rs-load (map
                  #(select-keys % [:server :load-info])
                  (hbase/get-servers-load (get-admin hb)))]
    (into []
      ;;map on server
      (mapcat
        (fn [x]
          (let [server (first (str/split (:host (:server x)) #"\." 2))
                metrics (dissoc (:load-info x) :rs-coprocessor :rs-coprocessor-region-level)]
            ;;map on metrics
            (map
              (fn [[k v]]
              {:service (str server "." (name k)) :metric v})
              metrics)))
          rs-load))))

(defn fetch-tables-load
  [hb]
  (let [t-load (hbase/get-tables-load (get-admin hb))]
    (into []
      ;;map on table
      (mapcat
      (fn [[tk tv]]
        ;;map on metrics
        (map
          (fn [[k v]]
            {:service (str (name tk) "." (name k)) :metric v})
             tv))
            t-load))))

(defn fetch-regionservers-tables-load
  [hb]
  (let [rst-load (hbase/get-servers-tables-load (get-admin hb))]
    (into []
      ;; map on servers
      (mapcat
        (fn [x]
          (let [server (first (str/split (:host x) #"\." 2))
                t-load (:tables-load x)]
              ;;map on table
              (mapcat
                (fn [[tk tv]]
                  ;;map on metrics
                  (map
                    (fn [[k v]]
                      {:service (str server "." (name tk) "." (name k)) :metric v})
                    tv))
                  t-load)))
          rst-load))))


(defn cluster-basics
  ([{:keys [quorum base-path period] :or {quorum "localhost:2181" base-path "/hbase"} :as conf}]
  (when-not (get @init? (str quorum base-path))
    (hbase/init-sc-hbase-connection quorum base-path)
    (swap! init? assoc (str quorum base-path) true))
  (fetch-cluster-basics (str quorum base-path)))
  ([] cluster-basics {}))

(defn regionservers-load
  ([{:keys [quorum base-path period] :or {quorum "localhost:2181" base-path "/hbase"} :as conf}]
  (when-not (get @init? (str quorum base-path))
    (hbase/init-sc-hbase-connection quorum base-path)
    (swap! init? assoc (str quorum base-path) true))
  (fetch-regionservers-load (str quorum base-path)))
  ([] regionservers-load {}))

(defn tables-load
  ([{:keys [quorum base-path period] :or {quorum "localhost:2181" base-path "/hbase"} :as conf}]
  (when-not (get @init? (str quorum base-path))
    (hbase/init-sc-hbase-connection quorum base-path)
    (swap! init? assoc (str quorum base-path) true))
  (fetch-tables-load (str quorum base-path)))
  ([] tables-load {}))

(defn regionservers-tables-load
  ([{:keys [quorum base-path period] :or {quorum "localhost:2181" base-path "/hbase"} :as conf}]
  (when-not (get @init? (str quorum base-path))
    (hbase/init-sc-hbase-connection quorum base-path)
    (swap! init? assoc (str quorum base-path) true))
  (fetch-regionservers-tables-load (str quorum base-path)))
  ([] regionservers-tables-load {}))
