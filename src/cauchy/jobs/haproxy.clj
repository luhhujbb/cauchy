(ns cauchy.jobs.haproxy
  (:require [clj-http.client :as http]))

(def state (atom {:state "warning"}))

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn haproxy-fetch-metrics
  "Get uberfeedr metrics"
  [{:keys [host port password] :or {host "localhost" port 443 password "admin:cl4rk3="}}]
    (let [url (str "https://"password"@" host ":" port "/haproxy-stats;csv")]
        (:body (http/get url {:insecure? true} ))))

(defn extract-fields []
  (first (map (fn [field] (clojure.string/split field #"," )) 
      (clojure.string/split (haproxy-fetch-metrics []) #"\n"))))

(defn kw-extract-fields []
  (vec (map (fn [x] (keyword (clojure.string/replace x  #"# " "" ))) (extract-fields))))

(defn get-raw-stats []
  (drop 1 (map (fn [x] (clojure.string/split x #"," )) 
      (clojure.string/split (haproxy-fetch-metrics []) #"\n"))))

(defn json-stats []
  (vec(map (fn [stats] (zipmap (kw-extract-fields) stats)) (get-raw-stats))))

(defn pretty-stats []
  (map 
     (fn [x]
        (let [{:keys [:weight status svname smax hrsp_2xx hrsp_3xx hrsp_4xx pxname] } x ]
          {:pxname pxname :status status :svname svname :smax smax :weight weight 
           :hrsp_2xx hrsp_2xx :hrsp_3xx hrsp_3xx :hrsp_4xx hrsp_4xx}))
  (json-stats) ))

(defn haproxy-stats []
 (vec 
    (map
       (fn [x]
      (let [{:keys [pxname status svname weight hrsp_3xx hrsp_2xx hrsp_4xx]} x]
         (cond
           (true? (= svname "BACKEND")) (reset! state "critical") :else (reset! state "warning"))
          {:service (str pxname"."(clojure.string/replace svname #"\." "_")".status") 
            :state (cond (true? (= status "DOWN")) @state :else "ok") 
            :metric (cond (true? (or(= status "UP")(= status "OPEN")) ) 1 :else 0)}
          ))(pretty-stats))))
