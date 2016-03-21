(ns cauchy.output.riemann
  (:require [riemann.client :as rc]
            [clojure.tools.logging :as log]))

(def rc (atom nil))

(defn init!
  [conf]
  (let [conf (:riemann conf)]
      (log/info "Riemann Output Service initializing with conf" conf)
      (reset! rc (rc/tcp-client conf))))

(defn send!
  [msg]
  (rc/send-event @rc msg))
