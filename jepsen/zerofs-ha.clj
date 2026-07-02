(ns jepsen.local-fs.db.zerofs-ha
  "HA ZeroFS: leader + standby over one shared file:// store, mounted
  multi-target so the FUSE client re-routes on failover. The fault is a leader
  FAILOVER (kill leader, standby promotes, full-restart to canonical roles).
  Semi-sync acks every write to the standby before acking, so failover loses no
  acked write; the checker models :failover as a no-loss flush."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [db :as db]
                    [util :refer [await-fn meh]]]
            [jepsen.local-fs [util :refer [sh]]]
            [jepsen.local-fs.db.core :refer [Failover]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def start-stop-daemon
  "/usr/sbin/start-stop-daemon")

(defn paths
  "Per-node state and the shared store live in a sibling <dir>.zerofs directory."
  [dir]
  (let [dir  (.getCanonicalPath (io/file dir))
        work (str dir ".zerofs")]
    {:mount      dir
     :work       work
     :store      (str work "/store")     ; shared by both nodes
     :mount-log  (str work "/mount.log")
     :mount-pid  (str work "/mount.pid")
     :nodes      {:a {:cache  (str work "/cache-a")
                      :config (str work "/a.toml")
                      :log    (str work "/a.log")
                      :pid    (str work "/a.pid")}
                  :b {:cache  (str work "/cache-b")
                      :config (str work "/b.toml")
                      :log    (str work "/b.log")
                      :pid    (str work "/b.pid")}}}))

(defn ninep-port [base node-key] (+ base (case node-key :a 0 :b 1)))
(defn repl-port  [base node-key] (+ base (case node-key :a 2 :b 3)))
(defn peer-key   [node-key]      (case node-key :a :b :b :a))

(defn node-config-str
  "One HA node's TOML. Symmetric replication (own listen port + peer's),
  fsync-is-the-only-flush LSM config."
  [{:keys [store password port] :as db} node-key role]
  (let [n (get-in db [:nodes node-key])]
    (str "[cache]\n"
         "dir = \"" (:cache n) "\"\n"
         "disk_size_gb = 1.0\n"
         "memory_size_gb = 0.25\n\n"
         "[storage]\n"
         "url = \"file://" store "\"\n"
         "encryption_password = \"" password "\"\n\n"
         "[servers.ninep]\n"
         "addresses = [\"127.0.0.1:" (ninep-port port node-key) "\"]\n\n"
         "[replication]\n"
         "node_id = \"" (name node-key) "\"\n"
         "role = \"" role "\"\n"
         "replication_listen = \"127.0.0.1:" (repl-port port node-key) "\"\n"
         "peers = [\"127.0.0.1:" (repl-port port (peer-key node-key)) "\"]\n\n"
         "[lsm]\n"
         "flush_interval_secs = 86400\n")))

(defn daemon-start! [bin pidfile log args]
  (sh :bash :-c
      (str start-stop-daemon " --start --background --no-close --make-pidfile"
           " --pidfile " pidfile " --exec " bin
           " -- " (str/join " " args) " >> " log " 2>&1")))

(defn pid [pidfile]
  (try (-> pidfile slurp str/trim parse-long)
       (catch java.io.IOException _ nil)
       (catch RuntimeException _ nil)))

(defn kill! [signal pidfile]
  (when-let [p (pid pidfile)]
    (meh (sh :kill (str "-" (name signal)) (str p)))))

(defn listening? [port]
  (try+ (sh :bash :-c (str "ss -ltn 'sport = :" port "' | grep -q LISTEN"))
        true
        (catch [:type :jepsen.local-fs.util/nonzero-exit] _ false)))

(defn mounted? [dir]
  (try+ (boolean (re-find #"zerofs" (sh :findmnt :-n dir)))
        (catch [:type :jepsen.local-fs.util/nonzero-exit] _ false)))

(defn mount-usable?
  "Can we list the mount? True once some node holds the lease and serves; the
  multi-target client routes to it."
  [dir]
  (try+ (sh :bash :-c (str "ls -A " dir " > /dev/null"))
        true
        (catch [:type :jepsen.local-fs.util/nonzero-exit] _ false)))

(defn unmount! [dir]
  (when (mounted? dir)
    (info "Unmounting" dir)
    (or (meh (sh :fusermount3 :-uz dir))
        (meh (sh :fusermount  :-uz dir)))))

(defn connected-count
  "Count of standby-connected log lines on node a. Logs accumulate across
  restarts, so callers wait for this to increase, not to be non-zero."
  [db]
  (try (->> (slurp (get-in db [:nodes :a :log]))
            (re-seq #"connected to standby")
            count)
       (catch java.io.IOException _ 0)))

(defn start-node! [db node-key role]
  (let [n (get-in db [:nodes node-key])]
    (info "Starting ZeroFS node" node-key "as" role)
    (spit (:config n) (node-config-str db node-key role))
    (daemon-start! (:bin db) (:pid n) (:log n) ["run" "-c" (:config n)])
    ; Only a leader binds 9P; a standby serves nothing until it promotes, so its
    ; readiness is observed via await-standby-connected!, not here.
    (when (= role "leader")
      (await-fn (fn [] (or (listening? (ninep-port (:port db) node-key))
                           (throw+ {:type ::node-not-listening :node node-key})))
                {:retry-interval 100 :log-interval 5000 :timeout 120000
                 :log-message (str "Waiting for ZeroFS leader " node-key " 9P")}))))

(defn mount! [db]
  (let [targets (str "127.0.0.1:" (ninep-port (:port db) :a)
                     ",127.0.0.1:" (ninep-port (:port db) :b))]
    (info "Mounting multi-target" targets "at" (:mount db))
    (daemon-start! (:bin db) (:mount-pid db) (:mount-log db)
                   ["mount" targets (:mount db) "--writeback" (str (:writeback db))])
    (await-fn (fn [] (or (and (mounted? (:mount db)) (mount-usable? (:mount db)))
                         (throw+ {:type ::not-mounted})))
              {:retry-interval 200 :log-interval 5000 :timeout 120000
               :log-message (str "Waiting for ZeroFS mount at " (:mount db))})))

(defn await-standby-connected!
  "Blocks until node a has logged >= n standby connections (semi-sync up)."
  [db n]
  (await-fn (fn [] (or (>= (connected-count db) n)
                       (throw+ {:type ::standby-not-connected})))
            {:retry-interval 200 :log-interval 5000 :timeout 120000
             :log-message "Waiting for standby to connect (semi-sync resumed)"}))

(defn start-cluster!
  "Start a=leader + b=standby, wait for the standby to connect (semi-sync
  active), then mount multi-target."
  [db]
  (let [base (connected-count db)]
    (start-node! db :a "leader")
    (start-node! db :b "standby")
    (await-standby-connected! db (inc base))
    (mount! db)))

(defn stop-cluster! [db]
  (unmount! (:mount db))
  (kill! :KILL (:mount-pid db))
  (kill! :KILL (get-in db [:nodes :a :pid]))
  (kill! :KILL (get-in db [:nodes :b :pid])))

(defrecord HADB [bin port password writeback mount work store
                 mount-log mount-pid nodes]
  db/DB
  (setup! [this _test _node]
    (info "Setting up ZeroFS HA cluster at" mount)
    (sh :mkdir :-p work store
        (get-in this [:nodes :a :cache]) (get-in this [:nodes :b :cache]) mount)
    (start-cluster! this)
    (info "ZeroFS HA cluster ready at" mount))

  (teardown! [this _test _node]
    (info "Tearing down ZeroFS HA cluster at" mount)
    (stop-cluster! this)
    (meh (sh :bash :-c (str "rm -rf " (pr-str work) " " (pr-str mount)))))

  Failover
  (failover! [this]
    ; Standby promotes, replaying + flushing the semi-synced tail to the store
    ; BEFORE serving, so no acked write is lost.
    (info "Failover: killing leader a; standby b must promote")
    (kill! :KILL (get-in this [:nodes :a :pid]))
    (await-fn (fn [] (or (mount-usable? mount)
                         (throw+ {:type ::no-promotion})))
              {:retry-interval 300 :log-interval 5000 :timeout 180000
               :log-message "Waiting for standby b to promote and serve"})
    ; Full-restart to canonical roles; start-cluster! gates on the standby
    ; reconnecting, so semi-sync is active again before the next op.
    (info "Failover: standby promoted; full-restarting to canonical roles")
    (stop-cluster! this)
    (start-cluster! this)
    :done))

(defn db
  "2-node HA ZeroFS Jepsen DB from CLI opts. Ports derive from --zerofs-port
  (a:port, b:port+1, repl a:port+2, b:port+3)."
  [{:keys [dir zerofs-bin zerofs-port zerofs-password zerofs-writeback]}]
  (let [bin (.getCanonicalPath (io/file zerofs-bin))]
    (when-not (.exists (io/file bin))
      (throw+ {:type ::no-binary
               :bin  bin
               :msg  (str "zerofs binary not found at " bin
                          "; build it (cargo build) or pass --zerofs-bin")}))
    (map->HADB (merge (paths dir)
                      {:bin       bin
                       :port      zerofs-port
                       :password  zerofs-password
                       :writeback (boolean zerofs-writeback)}))))
