(ns cauchy.output.riemann
  (:require [riemann.client :as r]
            [clojure.tools.logging :as log])
  (:import (java.io IOException)))

(def rc (atom nil))
(def r-conf (atom nil))
(def msg-count (atom 0))

(defn init!
  [conf]
    (reset! r-conf conf)
    (reset! rc (r/tcp-client conf)))

(defn send!
  [msg]
  (swap! msg-count inc)
  (when (= 0 (mod @msg-count (or (:nb-msg-before-reconnect r-conf) 50)))
    ;;since this happen only on network issue, we accept loss of 50 msgs
    (when-not (r/connected? @rc)
      (try (r/reconnect! @rc)
         (catch Exception e
           (log/error "[RIEMANN] fail to reconnect" e)))))
    (when (r/connected? @rc)
      (r/send-event @rc msg)))

(defn close!
  []
  (r/close! @rc)
  (reset! rc nil))
