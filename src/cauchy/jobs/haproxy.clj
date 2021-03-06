(ns cauchy.jobs.haproxy
  (:require [clj-http.client :as http]))

(def state (atom {:state "warning"}))

(def haproxy-fields [:pxname :svname :qcur :qmax :scur :smax :slim :stot 
                     :bin :bout :dreq :dresp :ereq :econ :eresp :wretr :wredis 
                     :status :weight :act :bck :chkfail :chkdown :lastchg :downtime 
                     :qlimit :pid :iid :sid :throttle :lbtot :tracked :type :rate :rate_lim 
                     :rate_max :check_status :check_code :check_duration :hrsp_1xx :hrsp_2xx 
                     :hrsp_3xx :hrsp_4xx :hrsp_5xx :hrsp_other :hanafail :req_rate :req_rate_max 
                     :req_tot :cli_abrt :srv_abrt :comp_in :comp_out :comp_byp :comp_rsp :lastsess 
                     :last_chk :last_agt :qtime :ctime :rtime :ttime])

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn haproxy-fetch-metrics
  [protocol host port password]
    (let [url (str protocol"://"password"@" host ":" port "/haproxy-stats;csv")]
        (:body (http/get url {:insecure? true} ))))

(defn json-stats [protocol host port password]
(remove nil?
(map 
  (fn [x] 
    (let [{:keys [:weight status svname smax hrsp_2xx hrsp_3xx hrsp_4xx pxname] } x ] 
        (cond
         (and
            (not= svname "FRONTEND")
            (not= svname "BACKEND"))
          {:pxname pxname :status status :svname svname :smax smax :weight weight 
           :hrsp_2xx hrsp_2xx :hrsp_3xx hrsp_3xx :hrsp_4xx hrsp_4xx})))
    (vec
      (map 
        (fn [stats] 
          (zipmap haproxy-fields stats))
          (drop 1 
          (map 
           (fn [x] (clojure.string/split x #"," )) 
            (clojure.string/split (haproxy-fetch-metrics protocol host port password) #"\n"))))))))

(defn haproxy-backend-stats [protocol host port password]
 (vec 
    (map
       (fn [x]
      (let [{:keys [pxname status svname weight hrsp_3xx hrsp_2xx hrsp_4xx]} x]
         (cond
           (true? (= svname "BACKEND")) (reset! state "critical") :else (reset! state "warning"))
          {:service (str pxname"."(clojure.string/replace svname #"\." "_")".status") 
            :state (cond (true? (= status "DOWN")) @state :else "ok") 
            :metric (cond (true? (or(= status "UP")(= status "OPEN")) ) 1 :else 0)}
          ))(json-stats protocol host port password))))

(defn haproxy-global-stats [protocol host port password]
  (into []
   (mapcat
     (fn [[mymap nb_backend ]]
      [{:service (str (:pxname mymap)"."(:status mymap)) 
        :state (cond (true? (= nb_backend 0)) "critical" :else "ok") 
        :metric nb_backend}
      (when-not (= "DOWN" (:status mymap)) 
       {:service (str (:pxname mymap)".DOWN") :metric 0 :state "ok" })
        ])
   (frequencies 
     (map 
       (fn [x] 
         (select-keys x [:status :pxname])) 
           (into [] (json-stats protocol host port password)))))))

(defn haproxy-stats [{:keys [protocol host port password] :or {protocol "http" host "localhost" port 443 password "notThisOne"}}]
  (into []
    (concat
      (haproxy-backend-stats protocol host port password)
      (haproxy-global-stats protocol host port password))))

