(ns cauchy.jobs.postfix
  (:use [clojure.java.shell :only [sh]]))

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn getmetric [queue] 
	(parse-int (:out (sh "bash" "-c" (format "find /var/spool/postfix/%s -mindepth 1| wc -l" queue )))))

(defn parse-postfix-metrics []
	(hash-map 
		:active (getmetric "active") 
		:corrupt (getmetric "corrupt")
		:incoming (getmetric "incoming")
		:saved (getmetric "saved")
		:hold (getmetric "hold")
		:bounce (getmetric "bounce")
		:deferred (getmetric "deferred")))

(defn postfix-metrics []
	(let [{:keys [:active :deferred :corrupt :incoming :saved :hold :bounce ]} (parse-postfix-metrics)]
       [{:service (str "active") :state "ok" :metric active} 
		{:service (str "corrupt") :state "ok" :metric corrupt}
		{:service (str "incoming") :state "ok" :metric incoming}
		{:service (str "saved") :state "ok" :metric saved}
		{:service (str "hold") :state "ok" :metric hold}
		{:service (str "bounce") :state "ok" :metric bounce}
        {:service (str "deferred") :state "ok" :metric deferred}])) 
