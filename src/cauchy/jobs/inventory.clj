(ns cauchy.jobs.inventory
  (:require [clj-http.client :as http]
            [clojure.tools.logging :as log]
            [cheshire.core :refer :all]
            [clojure.string :as str]))

(defn fetch-inventory-stats
  [host port tag filter]
  (let [url (str "http://" host ":" port "/inventory/stats/tag/resource/" tag)
        body (generate-string {:tags filter})
        resp (:body (try
                (http/post url {:throw-exceptions false
                                :content-type :json
                                :body body
                                :as :json})
                (catch Exception e
                  (log/info e)
                  {:body {:state "error"}})))]
        (if (= "success" (:state resp))
          (into [](map (fn [[k v]] {:service (name k) :metric v}) (:data resp)))
          [])))

(defn inventory-stats
  ([{:keys [host port tag filter]
     :or {host "127.0.0.1" port "8080" tag "state" filter []}
     :as conf}]
     (fetch-inventory-stats host port tag filter))
  ([] (inventory-stats {})))

(defn p->u
  [string]
  (str/replace string #"\." "_"))

(defn fetch-aws-instance-type-stats
  [host port]
  (let [url (str "http://" host ":" port "/aws/stats/instance/type")
        resp (:body (try
                (http/get url {:throw-exceptions false
                                :as :json})
                (catch Exception e
                  (log/info e)
                  {:body {:state "error"}})))]
    (if (= "success" (:state resp))
        (into [](map (fn [[k v]] {:service (p->u (name k)) :metric v}) (:data resp)))
        [])))

  (defn aws-instance-type-stats
    ([{:keys [host port]
       :or {host "127.0.0.1" port "8080"}
       :as conf}]
       (fetch-aws-instance-type-stats host port))
      ([](aws-instance-type-stats {})))
