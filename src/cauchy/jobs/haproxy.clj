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
  [host port password]
    (let [url (str "https://"password"@" host ":" port "/haproxy-stats;csv")]
        (:body (http/get url {:insecure? true} ))))

(defn json-stats [host port password]
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
            (clojure.string/split (haproxy-fetch-metrics host port password) #"\n"))))))))

(defn haproxy-backend-stats [host port password]
 (vec 
    (map
       (fn [x]
      (let [{:keys [pxname status svname weight hrsp_3xx hrsp_2xx hrsp_4xx]} x]
         (cond
           (true? (= svname "BACKEND")) (reset! state "critical") :else (reset! state "warning"))
          {:service (str pxname"."(clojure.string/replace svname #"\." "_")".status") 
            :state (cond (true? (= status "DOWN")) @state :else "ok") 
            :metric (cond (true? (or(= status "UP")(= status "OPEN")) ) 1 :else 0)}
          ))(json-stats host port password))))

(defn haproxy-global-stats [host port password]
  (into []
   (map
     (fn [[map nb_backend ]]
       {:service (str (:pxname map)"."(:status map)) 
        :state (cond (true? (= nb_backend 0)) "critical" :else "ok") 
        :metric nb_backend})
   (frequencies 
     (map 
       (fn [x] 
         (select-keys x [:status :pxname])) 
           (into [] (json-stats host port password)))))))

(defn haproxy-stats [{:keys [host port password] :or {host "localhost" port 443 password "notThisOne"}}]
  (into []
    (concat
      (haproxy-backend-stats host port password)
      (haproxy-global-stats host port password))))

