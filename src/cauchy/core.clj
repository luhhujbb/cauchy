(ns cauchy.core
  (:import [java.io File])
  (:require [clojure.tools.logging :as log]
            [bultitude.core :as bult]
            [indigenous.core :as indi]
            [cauchy.scheduler :as sch]
            [cauchy.output.riemann :as rn]
            [clojure.string :as str])
  (:gen-class))

(def locker (future "Cauchy successfully started"))

(defn load-sigar-native
  []
  (let [path (case (str (indi/os) "-" (indi/arch))
               "linux-x86"    "native/linux/x86/libsigar-x86-linux.so"
               "linux-x86_64" "native/linux/x86_64/libsigar-amd64-linux.so"
               "mac-x86"      "native/macosx/x86/libsigar-universal-macosx.dylib"
               "mac-x86_64"   "native/macosx/x86_64/libsigar-universal64-macosx.dylib"
               "win-x86"      "native/windows/x86/sigar-x86-winnt.dll"
               "win-x86_64"   "native/windows/x86_64/sigar-amd64-winnt.dll")]
    (indi/load-library "sigar" path)
    ;; We loaded OK. Now fire Sigar initialization.
    (require 'sigmund.core)))

(defn format-output*
  [defaults label job {:keys [service] :as job-output}]
  (let [ttl (* 2 (:interval job))
        final-service (if service
                        (str label "." service)
                        label)]
    (->> (merge defaults
                {:service final-service :ttl ttl}
                (dissoc job-output :service))
         (remove (fn [[k v]] (nil? v)))
         (into {}))))

(defn format-output
  [defaults label job job-output]
  (if (sequential? job-output)
    (map (partial format-output* defaults label job) job-output)
    [(format-output* defaults label job job-output)]))

(defn mk-fun
  [myns func args]
  (if myns
    ;; qualified function (by ns+name)
    (let [syms (bult/namespaces-on-classpath :prefix "cauchy.jobs")
          good-ns (first (filter (fn [sym] (= (str sym) myns)) syms))
          _ (require good-ns)
          func (ns-resolve good-ns (symbol func))]
      ;; return a thunk executing func using args
      (fn [] (apply func args)))
    ;; anonymous function defined in-line.
    (fn [] (apply (eval func) args))))





(defn clj-or-edn?
"Indicate if the string is a path for a clj or edn file"
 [str]
 (let [file-pattern #".*\.(?:clj|edn)"]
   (if-not (nil? (re-matches file-pattern str) )
    true
    false)))

(defn load-dir
  "Do like load-file but with all clj or edn file in dir, optionaly exclude "
 [path & [exclude-files]]
 (let [file-list (.listFiles (File. path))
       file-pattern #".*\.(?:clj|edn)"]
     (reduce (fn
                [acc file-path]
                (merge-with merge acc (load-file file-path)))
              {}
              (filter
                (fn [val](not (nil? val)))
                (map
                  (fn [file]
                    (when-not (some true? (map (fn[exclude] (.endsWith (.getAbsolutePath file) exclude)) exclude-files))
                      (do (log/info "loading" (.getAbsolutePath file))
                        (re-matches file-pattern (.getAbsolutePath file)))))
                  file-list)))))

(defn load-conf
  "load all clojure files (edn or clj) and merge them into a single map"
  [conf-paths]
    (let [files (str/split conf-paths #",")
          only-files (filter #(clj-or-edn? %) files)
          dirs (filter #(not (clj-or-edn? %)) files)
          conf (reduce
                (fn [acc file-path]
                    (do
                      (log/info "loading" file-path)
                      (merge-with merge acc (load-file file-path)))) {} only-files)]
          (reduce (fn [acc file-path]
                    (log/info "loading" file-path)
                    (merge-with merge acc (load-dir file-path only-files))) conf dirs)
      ))


(defn start!
  "main start function"
  [conf]
       (let [jobs (into {} (vals (:jobs conf)))
             defaults (assoc (:defaults conf)
                        :host (.. java.net.InetAddress
                                  getLocalHost
                                  getHostName))]
         (rn/init! conf)
         (load-sigar-native)
         (log/info "Cauchy Service start with jobs" jobs)

         (->> jobs
              (map (fn [[label {:keys [interval job-ns job-fn args]
                                 :as job}]]
                     (log/info "Scheduling job" job)
                     (let [active (get job :active true)
                           job-thunk (mk-fun job-ns job-fn args)
                           job-fn #(try
                                     (->> (job-thunk)
                                          (format-output defaults label job)
                                          (map rn/send!)
                                          (doall))
                                     (catch Exception e
                                       (log/error e "Job" label "failed")))]
                       {:label label
                        :active active
                        :interval interval
                        :job-fn job-fn})))
              (map sch/do-schedule)
              (doall))
         (log/info @locker)
         (fn []
           (sch/clear-scheduler)
           (shutdown-agents))
         ))

(defn -main
  "main"
  ([conf-files]
     (let [stop-fn (start! (load-conf conf-files))]
      (.addShutdownHook (Runtime/getRuntime)
                       (proxy [Thread] []
                         (run []
                           (log/info "Shutting down everything")
                           (stop-fn)))))))
