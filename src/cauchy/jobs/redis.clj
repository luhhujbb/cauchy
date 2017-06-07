(ns cauchy.jobs.redis
	(:use [clojure.java.shell :only [sh]]))

(def defaultstate "ok" )

(defn hitrate [kspace_hits kspace_misses] 
	(/ kspace_hits (+ kspace_hits kspace_misses)))

(defn getrawstats []
	(:out (sh "bash" "-c" "/usr/bin/redis-cli -h localhost -p 6379 \"info\" |grep -v ^# |grep -v ^db |grep -v ^os |grep -v config_file |sed -e 's/,//g'" )))

(defn mergetovec []
	(vec(remove empty?	(clojure.string/split (getrawstats) #"\r\n"))))

(defn mergetomap []
    (into {} (map (fn [s] ((juxt (comp keyword first) second) (clojure.string/split s #":")))  (mergetovec))))

(defn redismetrics []
	(let [{:keys [total_net_input_bytes total_net_output_bytes expired_keys 
		  evicted_keys used_memory_lua instantaneous_ops_per_sec rejected_connections keyspace_misses 
          keyspace_hits connected_clients rejected_connections total_commands_processed]} (mergetomap)]
     (cond (= instantaneous_ops_per_sec 0) (def state "warn"):else (def state "ok"))
     [{:service "total_net_input_bytes" :state defaultstate :metric total_net_input_bytes}
     {:service "total_net_output_bytes" :state defaultstate :metric total_net_output_bytes}
      {:service "expired_keys" :state defaultstate :metric expired_keys }
      {:service "evicted_keys" :state defaultstate :metric evicted_keys}
      {:service "used_memory_lua" :state defaultstate :metric used_memory_lua }
      {:service "instantaneous_ops_per_sec" :state state :metric instantaneous_ops_per_sec}
      {:service "rejected_connections" :state defaultstate :metric rejected_connections}
      {:service "keyspace_misses" :state defaultstate :metric keyspace_misses}
      {:service "keyspace_hits" :state defaultstate :metric keyspace_hits }
      {:service "total_commands_processed" :state defaultstate :metric total_commands_processed }
      {:service "connected_clients" :state defaultstate :metric connected_clients }
      {:service "hitrate" :state defaultstate :metric (float(hitrate (read-string keyspace_hits) (read-string keyspace_misses)))}]
))
