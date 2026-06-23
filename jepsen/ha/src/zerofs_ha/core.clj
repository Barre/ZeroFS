(ns zerofs-ha.core
  "Jepsen HA test for ZeroFS: local MinIO + a ZeroFS leader/standby pair over it +
  a multi-target FUSE mount. Nemesis kills leader/standby/both/MinIO then heals.
  Workload is a grow-only set (add = create+fsync a uniquely-named file, fsync
  being a global durability barrier; read = ls); set-full checker asserts no
  acked add is lost. All-local: one logical node, dummy SSH, process management
  via start-stop-daemon + shell rather than jepsen.control."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [client :as client]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [await-fn]]]
            [jepsen.os :as os]
            [slingshot.slingshot :refer [try+ throw+]])
  (:gen-class))

;; Local process + cluster management (no SSH).

(def start-stop-daemon "/usr/sbin/start-stop-daemon")

(defn sh!
  "Run a shell command, never throwing. Keyword args pass by name (:-n -> \"-n\")."
  [& args]
  (apply shell/sh (map (fn [a] (if (keyword? a) (name a) (str a))) args)))

(defn sh-ok!
  "Like sh! but throws on non-zero exit."
  [& args]
  (let [r (apply sh! args)]
    (when-not (zero? (:exit r))
      (throw+ {:type ::shell-failed :cmd args :result r}))
    r))

(defn cfg
  "Cluster config (paths, ports, binaries) from CLI opts."
  [opts]
  (let [work (:work-dir opts)]
    {:work        work
     :zerofs      (:zerofs-bin opts)
     :minio       (:minio-bin opts)
     :mc          (:mc-bin opts)
     :mount       (str work "/mnt")
     :minio-data  (str work "/minio-data")
     :minio-addr  "127.0.0.1:9000"
     :minio-log   (str work "/minio.log")
     :minio-pid   (str work "/minio.pid")
     :bucket      "zerofs-jepsen"
     :access-key  "minioadmin"
     :secret-key  "minioadmin"
     :password    "jepsen-ha"
     ;; Symmetric replication: each node listens on its repl-port and ships to a
     ;; RELAY (peer-port) that forwards to the peer's repl-port, so the partition
     ;; nemesis can cut the leader<->standby link without iptables.
     :nodes       {:a {:role "leader"  :repl-port 5582 :peer-port 6583
                       :cache (str work "/cache-a")
                       :ninep (str work "/a-9p.sock") :rpc (str work "/a-rpc.sock")
                       :log (str work "/a.log") :pid (str work "/a.pid")}
                   :b {:role "standby" :repl-port 5583 :peer-port 6582
                       :cache (str work "/cache-b")
                       :ninep (str work "/b-9p.sock") :rpc (str work "/b-rpc.sock")
                       :log (str work "/b.log") :pid (str work "/b.pid")}}
     ;; a ships to 6583 -> b's 5583; b ships to 6582 -> a's 5582. Cut = partition.
     :relays      {:to-b {:listen 6583 :target 5583}
                   :to-a {:listen 6582 :target 5582}}}))

(defn daemon-start!
  "Daemonize bin via start-stop-daemon (pid to pidfile, output to log). env is a
  map of extra environment variables."
  [pidfile log env bin args]
  (let [envs (->> env (map (fn [[k v]] (str k "=" v))) (str/join " "))]
    (sh-ok! :bash :-c
            (str envs " " start-stop-daemon
                 " --start --background --no-close --make-pidfile"
                 " --pidfile " pidfile
                 " --exec " bin
                 " -- " (str/join " " (map str args))
                 " >> " log " 2>&1"))))

(defn pid [pidfile]
  (try (-> pidfile slurp str/trim parse-long)
       (catch Exception _ nil)))

(defn proc-alive? [pidfile]
  (boolean (when-let [p (pid pidfile)]
             (zero? (:exit (sh! :kill :-0 (str p)))))))

(defn kill-pid! [pidfile]
  (when-let [p (pid pidfile)]
    (sh! :kill :-9 (str p))))

(defn pause-pid! [pidfile]
  (when-let [p (pid pidfile)]
    (sh! :kill :-STOP (str p))))

(defn resume-pid! [pidfile]
  (when-let [p (pid pidfile)]
    (sh! :kill :-CONT (str p))))

;; In-process TCP relay: network partitions
;; The leader<->standby replication (ship, heartbeat, Hello) all dial one TCP
;; port, so routing it through a relay we control and cutting the relay is a true
;; partition: both nodes stay up, keep serving clients (unix sockets) and reach
;; MinIO, but cannot reach each other. A transparent byte-pump, so gRPC passes.

(defn close-quietly! [^java.io.Closeable s] (try (.close s) (catch Exception _ nil)))

(defn pump!
  "Copy in -> out until EOF or error."
  [^java.io.InputStream in ^java.io.OutputStream out]
  (try
    (let [buf (byte-array 65536)]
      (loop []
        (let [n (.read in buf)]
          (when (pos? n)
            (.write out buf 0 n)
            (.flush out)
            (recur)))))
    (catch Exception _ nil)))

(defn start-relay!
  "TCP relay 127.0.0.1:listen -> 127.0.0.1:target. Returns {:cut! :heal! :stop!}:
  cut! closes live conns and refuses new ones (partition); heal! resumes new
  conns; stop! tears it down."
  [listen target]
  (let [blocked? (atom false)
        active   (atom #{})
        running  (atom true)
        server   (doto (java.net.ServerSocket.)
                   (.setReuseAddress true)
                   (.bind (java.net.InetSocketAddress. "127.0.0.1" (int listen))))]
    (future
      (while @running
        (when-let [client (try (.accept server) (catch Exception _ nil))]
          (swap! active conj client) ; track before the block-check so cut! catches it
          (if @blocked?
            (do (close-quietly! client) (swap! active disj client))
            (future
              (if-let [tgt (try (java.net.Socket. "127.0.0.1" (int target))
                                (catch Exception _ nil))]
                (let [^java.net.Socket c client
                      ^java.net.Socket t tgt]
                  (swap! active conj tgt)
                  (let [c->t (future (pump! (.getInputStream c) (.getOutputStream t)))
                        t->c (future (pump! (.getInputStream t) (.getOutputStream c)))]
                    @c->t @t->c)
                  (close-quietly! client) (close-quietly! tgt)
                  (swap! active disj client tgt))
                (do (close-quietly! client) (swap! active disj client))))))))
    {:cut!  (fn [] (reset! blocked? true) (doseq [s @active] (close-quietly! s)))
     :heal! (fn [] (reset! blocked? false))
     :stop! (fn []
              (reset! running false)
              (reset! blocked? true)
              (doseq [s @active] (close-quietly! s))
              (close-quietly! server))}))

;; Started in setup!: {:to-b <relay> :to-a <relay>}; the nemesis cuts/heals them.
(def relays (atom nil))

(defn minio-up? [c]
  (zero? (:exit (sh! :curl :-fsS (str "http://" (:minio-addr c) "/minio/health/live")))))

(defn start-minio! [c]
  (info "Starting MinIO")
  (daemon-start! (:minio-pid c) (:minio-log c)
                 {"MINIO_ROOT_USER" (:access-key c)
                  "MINIO_ROOT_PASSWORD" (:secret-key c)}
                 (:minio c)
                 ["server" (:minio-data c) "--address" (:minio-addr c)
                  "--console-address" "127.0.0.1:9001"])
  (await-fn (fn [] (or (minio-up? c) (throw+ {:type ::minio-down})))
            {:retry-interval 200 :log-interval 5000 :log-message "Waiting for MinIO"}))

(defn make-bucket! [c]
  (sh! (:mc c) "alias" "set" "j" (str "http://" (:minio-addr c)) (:access-key c) (:secret-key c))
  (sh! (:mc c) "mb" "--ignore-existing" (str "j/" (:bucket c))))

(defn node-cfg-str [c node-key role]
  (let [n (get-in c [:nodes node-key])]
    (str "[cache]\n"
         "dir = \"" (:cache n) "\"\n"
         "disk_size_gb = 1.0\n\n"
         "[storage]\n"
         "url = \"s3://" (:bucket c) "/data\"\n"
         "encryption_password = \"" (:password c) "\"\n\n"
         "[servers]\n\n[servers.ninep]\n"
         "unix_socket = \"" (:ninep n) "\"\n\n"
         "[servers.rpc]\n"
         "unix_socket = \"" (:rpc n) "\"\n\n"
         "[replication]\n"
         "node_id = \"" (name node-key) "\"\n"
         "role = \"" role "\"\n"
         "replication_listen = \"127.0.0.1:" (:repl-port n) "\"\n"
         "peers = [\"127.0.0.1:" (:peer-port n) "\"]\n\n"
         "[aws]\n"
         "access_key_id = \"" (:access-key c) "\"\n"
         "secret_access_key = \"" (:secret-key c) "\"\n"
         "endpoint = \"http://" (:minio-addr c) "\"\n"
         "region = \"us-east-1\"\n"
         "allow_http = \"true\"\n"
         "conditional_put = \"etag\"\n\n"
         "[telemetry]\nenabled = false\n")))

(defn node-cfg-path [c node-key]
  (str (:work c) "/" (name node-key) ".toml"))

(defn start-node! [c node-key role]
  (let [n    (get-in c [:nodes node-key])
        path (node-cfg-path c node-key)]
    (info "Starting ZeroFS node" node-key "as" role)
    (spit path (node-cfg-str c node-key role))
    (daemon-start! (:pid n) (:log n) {} (:zerofs c) ["run" "-c" path])))

(defn node-9p-up? [c node-key]
  (.exists (io/file (get-in c [:nodes node-key :ninep]))))

(defn mounted? [c]
  (zero? (:exit (sh! :findmnt :-n (:mount c)))))

(defn mount-targets [c]
  (str "unix:" (get-in c [:nodes :a :ninep]) ",unix:" (get-in c [:nodes :b :ninep])))

(defn mount! [c]
  (info "Mounting at" (:mount c))
  (daemon-start! (str (:work c) "/mount.pid") (str (:work c) "/mount.log") {}
                 (:zerofs c)
                 ["mount" (mount-targets c) (:mount c) "--writeback" "false"])
  (await-fn (fn [] (or (mounted? c) (throw+ {:type ::not-mounted})))
            {:retry-interval 200 :log-interval 5000 :log-message "Waiting for mount"}))

(defn unmount! [c]
  (when (mounted? c)
    (or (zero? (:exit (sh! :fusermount3 :-uz (:mount c))))
        (sh! :fusermount :-uz (:mount c)))))

(defn cluster-down! [c]
  (unmount! c)
  (kill-pid! (str (:work c) "/mount.pid"))
  (kill-pid! (get-in c [:nodes :a :pid]))
  (kill-pid! (get-in c [:nodes :b :pid]))
  (kill-pid! (:minio-pid c)))

;; Live role tracking; the nemesis updates it as it kills/heals so :kill-leader
;; always targets the *current* leader (roles swap after a rejoin-as-standby).

(def cluster-roles (atom {:a :leader :b :standby}))

(defn node-pid [c k] (get-in c [:nodes k :pid]))
(defn role-node [role] (some (fn [[k r]] (when (= r role) k)) @cluster-roles))
(defn leader-node []  (role-node :leader))
(defn standby-node [] (role-node :standby))
(defn dead-node []    (role-node :dead))
(defn paused-node []  (role-node :paused))

(defn db []
  (reify db/DB
    (setup! [_ test _node]
      (let [c (cfg test)]
        (info "Setting up ZeroFS HA cluster")
        (cluster-down! c)
        (Thread/sleep 1000)
        (sh! :bash :-c (str "rm -rf " (:minio-data c) " " (get-in c [:nodes :a :cache])
                            " " (get-in c [:nodes :b :cache]) " " (:work c) "/*.log "
                            (:work c) "/*.sock"))
        (sh! :mkdir :-p (:minio-data c) (get-in c [:nodes :a :cache])
             (get-in c [:nodes :b :cache]) (:mount c))
        (start-minio! c)
        (make-bucket! c)
        (let [rs (:relays c)]
          (reset! relays
                  {:to-b (start-relay! (get-in rs [:to-b :listen]) (get-in rs [:to-b :target]))
                   :to-a (start-relay! (get-in rs [:to-a :listen]) (get-in rs [:to-a :target]))}))
        (start-node! c :a "leader")
        (await-fn (fn [] (or (node-9p-up? c :a) (throw+ {:type ::leader-down})))
                  {:retry-interval 200 :log-interval 5000 :log-message "Waiting for leader 9P"})
        (start-node! c :b "standby")
        (Thread/sleep 4000)
        (mount! c)
        (reset! cluster-roles {:a :leader :b :standby})
        (info "ZeroFS HA cluster ready")))

    (teardown! [_ test _node]
      (let [c (cfg test)]
        (info "Tearing down ZeroFS HA cluster")
        (cluster-down! c)
        (when-let [r @relays]
          ((:stop! (:to-b r)))
          ((:stop! (:to-a r)))
          (reset! relays nil))
        (sh! :bash :-c (str "rm -rf " (:mount c)))))))

;; Client: a set on the mount spread across `ndirs` subdirectories (exercising
;; several per-directory cookie counters). add = write a temp then atomically
;; rename it into place (no torn file, and exercises rename); remove = delete;
;; read = recursive ls + statfs + content scan. Each value is globally unique and
;; removed only by the worker that added it, so a value is never added and removed
;; concurrently: the checker decides each value's fate from its own ops, in order.

(def ndirs 16)

(defn subdir [dir v] (io/file dir (str "d" (mod v ndirs))))

(defn add-file!
  "Create value v with content \"v\\n\": write a temp then atomically rename it into
  place (so v appears only fully-written, and the rename path is exercised). The
  caller fsyncs AFTER this (a ZeroFS fsync is a global flush, so it must run after
  the rename to make the rename durable; fsyncing only the temp would leave the
  rename un-flushed and losable on a partition)."
  [dir v]
  (let [sd  (subdir dir v)
        tmp (io/file sd (str ".tmp-" v))
        f   (io/file sd (str v))]
    (with-open [out (java.io.FileOutputStream. tmp)]
      (.write out (.getBytes (str v "\n")))
      (.flush out))
    (java.nio.file.Files/move
     (.toPath tmp) (.toPath f)
     (into-array java.nio.file.CopyOption
                 [java.nio.file.StandardCopyOption/ATOMIC_MOVE]))))

(defn remove-file!
  "Delete value v (idempotent: a missing file still leaves v absent, the intent).
  The caller fsyncs after, so the deletion is durable (else a partition could lose
  the delete and resurrect v)."
  [dir v]
  (java.nio.file.Files/deleteIfExists (.toPath (io/file (subdir dir v) (str v)))))

(defn fsync-sentinel!
  "Fsync the sentinel file. A ZeroFS fsync is a GLOBAL flush, so calling it after a
  rename/delete makes that mutation durable on the shared store, so an :ok add or
  remove genuinely survives a failover (a partition included), not just the store's
  periodic flush or replication."
  [dir]
  (with-open [raf (java.io.RandomAccessFile. (io/file dir ".sync") "rw")]
    (.sync (.getFD raf))))

(defn integer-files
  "All integer-named regular files under dir (recursive), as [file name-long]."
  [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (keep (fn [f] (when-let [n (try (Long/parseLong (.getName f)) (catch Exception _ nil))]
                       [f n])))))

(defn cleanup-temps!
  "Delete leftover .tmp-* files (adds whose rename a fault interrupted) so the final
  statfs/read see only real set files."
  [dir]
  (->> (file-seq (io/file dir))
       (filter #(and (.isFile %) (.startsWith (.getName %) ".tmp-")))
       (run! #(.delete %))))

(defn read-set
  "Integers currently present as files anywhere under dir."
  [dir]
  (into (sorted-set) (map second) (integer-files dir)))

(defn statfs-used-inodes
  "Inodes the filesystem's usage stats account for, via statfs (`files - ffree`).
  ZeroFS derives `files`/`ffree` from its global usage stats + the inode-id
  allocator, so a takeover that regressed either (by replaying an already-flushed
  tail) under-reports here: an accounting leak the name-only set check can't see."
  [dir]
  (let [{:keys [out exit]} (sh! :stat :-f "--format=%c %d" (str dir))]
    (when-not (zero? exit)
      (throw (java.io.IOException. (str "statfs failed (" exit "): " out))))
    (let [[total free] (->> (str/split (str/trim out) #"\s+") (mapv bigint))]
      (long (- total free)))))

(defn corrupt-files
  "Integer-named files whose content isn't \"<name>\\n\", e.g. two names that
  ended up sharing an inode after an inode-id allocator regression. Returns names."
  [dir]
  (->> (integer-files dir)
       (keep (fn [[f n]]
               (when (not= (str n "\n") (try (slurp f) (catch Exception _ ::read-error)))
                 n)))
       (into (sorted-set))))

;; `counter` assigns globally-unique add values; `live` maps each worker (process)
;; to the values it has added and not yet removed, so a remove only ever targets a
;; value this same worker added, so no concurrent add/remove of one value. Both are
;; shared (open! returns `this`) across workers.
(defrecord SetClient [dir fsync? counter live]
  client/Client
  (open! [this _test _node] this)
  (setup! [_ _test]
    ;; Pre-create the subdirs + the fsync sentinel BEFORE the baseline read, so the
    ;; statfs baseline already counts them and final-minus-baseline growth is files.
    (dotimes [i ndirs] (.mkdirs (io/file dir (str "d" i))))
    (.createNewFile (io/file dir ".sync")))
  (invoke! [_ _test op]
    ;; Bound every op: during a full outage a FUSE op blocks until recovery and
    ;; would wedge the run. Timeout -> :info (indeterminate, never counted lost).
    (util/timeout 30000 (assoc op :type :info :error :timeout)
      (case (:f op)
        ;; Assign a unique value, record it under this worker, then create it.
        :add (let [v (swap! counter inc)
                   f (io/file (subdir dir v) (str v))]
               (swap! live update (:process op) (fnil conj #{}) v)
               (try
                 (add-file! dir v)
                 (when fsync? (fsync-sentinel! dir))
                 ;; Re-verify on the CURRENT node by READING v back: the add is
                 ;; multi-step (write tmp, rename, fsync) and over the failover mount a
                 ;; reroute can land those steps on different nodes, so :ok must mean
                 ;; v's CONTENT is durably here. Presence alone isn't enough: a reroute
                 ;; can land the dentry/rename without the (un-fsynced) data write,
                 ;; leaving an empty file. If the content isn't "v\n" here, durability
                 ;; is indeterminate -> :info (checker skips it), not a false :ok.
                 (if (= (str v "\n") (slurp f))
                   (assoc op :type :ok :value v)
                   (assoc op :type :info :value v :error :content-not-durable-here))
                 (catch java.io.IOException e
                   (let [msg (str (.getMessage e))]
                     (cond
                       (re-find #"(?i)file exists" msg)
                       (assoc op :type :info :value v :error :retried-create)
                       (re-find #"(?i)no such file|not found" msg)
                       (assoc op :type :info :value v :error :not-durable-here)
                       :else (assoc op :type :fail :value v :error msg))))))
        ;; Remove one of THIS worker's live values (none queued -> nothing to do).
        :remove (if-let [v (first (get @live (:process op)))]
                  (let [f (io/file (subdir dir v) (str v))]
                    (swap! live update (:process op) disj v)
                    (try
                      (remove-file! dir v)
                      (when fsync? (fsync-sentinel! dir))
                      ;; Re-verify absence on the CURRENT node (a reroute can split
                      ;; the delete and the fsync): :ok must mean v is really gone
                      ;; here. If a fresh open still finds it, the delete's durability
                      ;; is indeterminate -> :info, not a false :ok-removed.
                      (let [present? (try (.close (java.io.FileInputStream. f)) true
                                          (catch java.io.FileNotFoundException _ false))]
                        (if present?
                          (assoc op :type :info :value v :error :still-present-here)
                          (assoc op :type :ok :value v)))
                      (catch java.io.IOException e
                        (assoc op :type :info :value v :error (.getMessage e)))))
                  (assoc op :type :fail :error :nothing-to-remove))
        ;; read carries three signals on one op so the set checker only sees :add/
        ;; :remove/:read: :value = the live name set; :used-inodes = statfs
        ;; accounting; :corrupt = files whose content != name.
        :read (try (assoc op :type :ok
                          :value (read-set dir)
                          :used-inodes (statfs-used-inodes dir)
                          :corrupt (corrupt-files dir))
                   (catch java.io.IOException e
                     (assoc op :type :fail :error (.getMessage e))))
        ;; Drop leftover temps (interrupted renames) before the final read/statfs.
        :cleanup (try (cleanup-temps! dir) (assoc op :type :ok)
                      (catch java.io.IOException e
                        (assoc op :type :fail :error (.getMessage e)))))))
  (teardown! [_ _test])
  (close! [_ _test]))

;; Nemesis: kill/bounce leader, kill standby/both/minio, pause+fence, the
;; liveness re-take (promoted standby restarts while the leader is down), and heal.

(defn heal-restart!
  "Clean full restart to canonical leader=:a, standby=:b. Works from any state
  (incl. kill-both); the multi-target client re-routes."
  [c]
  (when-not (minio-up? c) (start-minio! c))
  (kill-pid! (node-pid c :a))
  (kill-pid! (node-pid c :b))
  (Thread/sleep 1000)
  (start-node! c :a "leader")
  (await-fn (fn [] (or (node-9p-up? c :a) (throw+ {:type ::leader-down})))
            {:retry-interval 200 :log-interval 5000 :log-message "heal: waiting for leader"})
  (start-node! c :b "standby")
  (Thread/sleep 4000)
  (await-fn (fn [] (or (mounted? c) (throw+ {:type ::not-mounted})))
            {:retry-interval 500 :log-interval 5000 :log-message "heal: waiting for mount"})
  (reset! cluster-roles {:a :leader :b :standby}))

(defn heal-rejoin!
  "Heal with no outage: bring the dead node back as STANDBY under the live leader
  (re-establishes shipping). Falls back to full restart when not a clean
  one-dead/one-leader state (e.g. after kill-both)."
  [c]
  (let [dead (dead-node) ldr (leader-node)]
    (if (and dead ldr (not= dead ldr))
      (do (info "heal: rejoining" dead "as standby under leader" ldr)
          (start-node! c dead "standby")
          (Thread/sleep 4000)
          (await-fn (fn [] (or (mounted? c) (throw+ {:type ::not-mounted})))
                    {:retry-interval 500 :log-interval 5000 :log-message "heal: waiting for mount"})
          (swap! cluster-roles assoc dead :standby))
      (heal-restart! c))))

(defn ha-nemesis []
  (reify nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (let [c (cfg test)]
        (assoc op :value
               (case (:f op)
                 :kill-leader  (let [n (leader-node) s (standby-node)]
                                 (kill-pid! (node-pid c n))
                                 (swap! cluster-roles assoc n :dead)
                                 (when s (swap! cluster-roles assoc s :leader))
                                 (str "killed-leader-" (name n)))
                 :kill-standby (let [n (standby-node)]
                                 (kill-pid! (node-pid c n))
                                 (swap! cluster-roles assoc n :dead)
                                 (str "killed-standby-" (name n)))
                 :kill-both    (do (kill-pid! (node-pid c :a))
                                   (kill-pid! (node-pid c :b))
                                   (reset! cluster-roles {:a :dead :b :dead})
                                   :killed-both)
                 :pause-minio  (do (pause-pid! (:minio-pid c)) :paused-minio)
                 :resume-minio (do (resume-pid! (:minio-pid c)) :resumed-minio)
                 ;; SIGSTOP the leader past its lease so the standby promotes +
                 ;; fences it; on thaw (resume-leader) the stale leader must fence
                 ;; ITSELF (expired lease + superseded epoch), not serve/corrupt.
                 :pause-leader  (let [n (leader-node) s (standby-node)]
                                  (pause-pid! (node-pid c n))
                                  (swap! cluster-roles assoc n :paused)
                                  (when s (swap! cluster-roles assoc s :leader))
                                  (str "paused-leader-" (name n)))
                 :resume-leader (let [p (paused-node) ldr (leader-node)]
                                  (if (and p ldr (not= p ldr))
                                    (do (info "resume: thawing stale leader" p
                                              "-- must fence itself under new leader" ldr)
                                        (resume-pid! (node-pid c p))
                                        (Thread/sleep 10000) ; window for the stale node to (not) serve/corrupt
                                        (info "resume: stale leader" p
                                              (if (proc-alive? (node-pid c p))
                                                "still up (should be refusing)" "self-fenced (exited)"))
                                        (kill-pid! (node-pid c p))   ; discard the fenced zombie
                                        (start-node! c p "standby")
                                        (Thread/sleep 4000)
                                        (await-fn (fn [] (or (mounted? c) (throw+ {:type ::not-mounted})))
                                                  {:retry-interval 500 :log-interval 5000
                                                   :log-message "resume: waiting for mount"})
                                        (swap! cluster-roles assoc p :standby)
                                        (str "resumed-" (name p)))
                                    (do (heal-restart! c) "resume-fell-back-to-restart")))
                 ;; Bounce the leader (kill + fast restart, under takeover_ttl): the
                 ;; restarted leader must DEFER (its Hello sees the standby active)
                 ;; and let the standby take over (active trigger), not re-take and
                 ;; drop the tail. With an active workload the standby holds a tail,
                 ;; so it promotes; roles flip (heal-restart resets them regardless).
                 :bounce-leader (let [n (leader-node) s (standby-node)]
                                  (kill-pid! (node-pid c n))
                                  (Thread/sleep 500)
                                  (start-node! c n "leader")
                                  (when s (swap! cluster-roles assoc s :leader))
                                  (swap! cluster-roles assoc n :standby)
                                  (str "bounced-leader-" (name n)))
                 ;; Liveness: kill the leader (leave it dead), let the standby
                 ;; promote, then restart the promoted standby. It must RE-TAKE (its
                 ;; Hello sees the original leader dead), not stall in the standby
                 ;; watch. The re-take await fails the test if it stalls; it also
                 ;; exercises store re-open after a crash on MinIO (resolves whether
                 ;; the local-fs "error transforming block" is a file:// artifact).
                 :liveness-restart (let [n (leader-node) s (standby-node)]
                                     (kill-pid! (node-pid c n))
                                     (swap! cluster-roles assoc n :dead)
                                     (swap! cluster-roles assoc s :leader)
                                     (await-fn (fn [] (or (node-9p-up? c s)
                                                          (throw+ {:type ::no-promote})))
                                               {:retry-interval 500 :log-interval 5000 :timeout 40000
                                                :log-message "liveness: waiting for standby to promote"})
                                     (Thread/sleep 3000)
                                     ;; Restart the promoted standby while the original leader is dead.
                                     (kill-pid! (node-pid c s))
                                     (sh! :rm :-f (get-in c [:nodes s :ninep]))
                                     (Thread/sleep 500)
                                     (start-node! c s "standby")
                                     (await-fn (fn [] (or (node-9p-up? c s)
                                                          (throw+ {:type ::no-retake})))
                                               {:retry-interval 500 :log-interval 5000 :timeout 60000
                                                :log-message "liveness: promoted standby must re-take"})
                                     (str "liveness-restarted-" (name s)))
                 :heal-rejoin  (do (heal-rejoin! c) :healed-rejoin)
                 ;; Network partition (no iptables): cut the relays carrying ALL
                 ;; leader<->standby traffic. Both nodes stay up + serve clients +
                 ;; reach MinIO but isolate from each other -> the standby promotes
                 ;; and the old leader, on its next flush, is fenced (epoch CAS) and
                 ;; exits. Exercises split-brain safety + no acked loss.
                 :partition  (let [r @relays]
                               ((:cut! (:to-b r)))
                               ((:cut! (:to-a r)))
                               :partitioned)
                 :heal-partition (let [r @relays]
                                   ((:heal! (:to-b r)))
                                   ((:heal! (:to-a r)))
                                   (heal-restart! c) ; restore connectivity, reset to canonical
                                   :healed-partition)
                 :heal-restart (do (heal-restart! c) :healed-restart)))))
    (teardown! [_ _test])))

;; Each scenario pairs a fault with a heal. MinIO is paused/resumed, not killed:
;; SIGKILL+restart of single-drive MinIO can lose acked PUTs (no fsync) and look
;; like ZeroFS data loss, whereas a real object store stays durable.
(def scenarios
  [{:fault :kill-leader  :heal :heal-rejoin}
   {:fault :kill-standby :heal :heal-rejoin}
   {:fault :kill-leader  :heal :heal-restart}
   {:fault :kill-standby :heal :heal-restart}
   {:fault :kill-both    :heal :heal-restart}
   {:fault :pause-minio  :heal :resume-minio}
   ;; Fencing test: freeze the leader well past lease+takeover (~3s) so the
   ;; standby promotes + fences it, then thaw and verify it self-fences.
   {:fault :pause-leader :heal :resume-leader :hold 32}
   ;; Leadership handoff (dynamic Hello-based election): a bounced leader defers
   ;; and the standby takes over; a promoted standby that restarts while the
   ;; original leader stays down must re-take. The op bodies do their own waits,
   ;; so a short hold suffices before the heal.
   {:fault :bounce-leader    :heal :heal-restart :hold 8}
   {:fault :liveness-restart :heal :heal-restart :hold 6}
   ;; Network partition. CONFIRMED RED (2026-06-22): a partitioned leader serves
   ;; Solo and acks fsync'd writes for ~2s until it detects the takeover ("newer
   ;; DB client") and exits; the promoted standby's state lacks them -> acked loss.
   ;; Tracking a real split-brain finding (not a heal-restart artifact).
   {:fault :partition       :heal :heal-partition :hold 12}])

(defn nemesis-gen
  "Cycle scenarios; each fault held :hold seconds (default 20, well past the 2s
  takeover_ttl so a leader fault triggers a real promotion) then its paired heal."
  []
  (->> (for [s scenarios]
         [(gen/sleep 6) {:type :info :f (:fault s)}
          (gen/sleep (:hold s 20)) {:type :info :f (:heal s)}])
       (apply concat)
       cycle))

(defn set-checker
  "Fault-tolerant add/remove set check (replaces the grow-only set-full now that we
  delete too). Each value is unique and is added then maybe removed by ONE worker in
  sequence, so its ops have a definite order with no concurrency. Replay them per
  value: :ok add -> present, :ok remove -> absent, :info -> indeterminate, :fail ->
  no change. A value ending 'present' must be in the final read (else LOST); one
  ending 'absent' must not be (else a removed value was RESURRECTED, e.g. a failover
  replaying a stale state). Indeterminate values are not asserted either way."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-set (or (->> history
                               (filter #(and (= :ok (:type %)) (= :read (:f %))))
                               last :value)
                          (sorted-set))
            by-v (->> history
                      (filter #(and (#{:ok :info :fail} (:type %))
                                    (#{:add :remove} (:f %))
                                    (some? (:value %))))
                      (group-by :value))
            final-state (fn [ops]
                          (reduce (fn [st op]
                                    (case [(:type op) (:f op)]
                                      [:ok :add]    :present
                                      [:ok :remove] :absent
                                      ([:info :add] [:info :remove]) :unknown
                                      st))
                                  :absent ops))
            states      (into {} (map (fn [[v ops]] [v (final-state ops)])) by-v)
            present-req (keep (fn [[v s]] (when (= s :present) v)) states)
            absent-req  (keep (fn [[v s]] (when (= s :absent)  v)) states)
            lost        (->> present-req (remove final-set) sort vec)
            resurrected (->> absent-req  (filter final-set) sort vec)]
        {:valid?            (and (empty? lost) (empty? resurrected))
         :present-required  (count present-req)
         :final-set-size    (count final-set)
         :lost-count        (count lost)
         :lost              (vec (take 50 lost))
         :resurrected-count (count resurrected)
         :resurrected       (vec (take 50 resurrected))}))))

(defn statfs-checker
  "The growth in statfs-reported used inodes (a baseline read on the empty fs vs
  the final read, after temp cleanup) must equal the number of files in the final
  live set: every live file is one inode, adds increment and removes decrement, so
  the net equals the final count. A takeover that regressed the usage stats /
  inode-id allocator, or mishandled a delete's decrement, mis-counts here even when
  `ls` (the set checker) still passes."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [reads (->> history
                       (filter #(and (= :ok (:type %)) (= :read (:f %))))
                       vec)
            baseline (some-> (first reads) :used-inodes)
            final    (some-> (last reads) :used-inodes)
            n        (some-> (last reads) :value count)]
        (if (or (nil? baseline) (nil? final) (nil? n) (< (count reads) 2))
          {:valid? :unknown
           :reason "need a baseline read (empty fs) and a final read"}
          {:valid?        (= (- final baseline) n)
           :baseline-used baseline
           :final-used    final
           :inodes-grew   (- final baseline)
           :files-in-set  n})))))

(defn content-checker
  "No CLEANLY-added file may have content that disagrees with its name: catches
  inode-id reuse / data corruption the name-only set checker can't see. \"Cleanly
  added\" = an :ok add that actually wrote, excluding `:retried-create` (a resent
  create whose write the first attempt may have left incomplete) and :info/:fail
  adds: a create whose write a fault interrupted leaves an empty file, which is
  POSIX-legal (the write was never durable), not corruption."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [corrupt (->> history
                         (filter #(and (= :ok (:type %)) (= :read (:f %))))
                         (mapcat :corrupt)        ; integer names (longs)
                         (into (sorted-set)))
            cleanly-added (->> history
                               (filter #(and (= :ok (:type %)) (= :add (:f %))
                                             (not (:retried-create %))))
                               (map :value)
                               (into (sorted-set)))
            real (filterv cleanly-added corrupt)]
        {:valid?             (empty? real)
         :corrupt-count      (count real)
         :corrupt            (vec (take 50 real))
         :incomplete-ignored (- (count corrupt) (count real))}))))

(defn ha-test [opts]
  (merge tests/noop-test
         opts
         {:name      "zerofs-ha"
          :os        os/noop
          :db        (db)
          :client    (->SetClient (str (:work-dir opts) "/mnt") (:fsync opts)
                                   (atom -1) (atom {}))
          :nemesis   (ha-nemesis)
          :checker   (checker/compose
                      ;; :set  -> no :ok add lost, no :ok-removed value resurrected.
                      ;; :statfs -> the usage-stats inode count tracks the live set
                      ;;            (catches a regressed stats/allocator or a bad delete).
                      ;; :content -> no file's content disagrees with its name
                      ;;             (catches inode-id reuse the set check can't see).
                      {:set     (set-checker)
                       :statfs  (statfs-checker)
                       :content (content-checker)
                       :perf    (checker/perf)})
          :generator (if (:fsync opts)
                       ;; fsync'd: full fault suite. Every acked add is durable on the
                       ;; shared store, so this checks store durability across all faults.
                       (gen/phases
                        ;; Baseline read on the (fresh) empty fs: its :used-inodes is
                        ;; the statfs zero point the final read is compared against.
                        (gen/clients (gen/once {:f :read}))
                        (->> (gen/mix [(repeat {:f :add}) (repeat {:f :add})
                                       (repeat {:f :remove})]) ; ~2/3 add, 1/3 remove
                             ;; No rate limit: per-op fsync latency bounds throughput,
                             ;; and faster = more ops inside each fault window = more coverage.
                             (gen/nemesis (nemesis-gen))
                             (gen/time-limit (:time-limit opts)))
                        ;; Heal + quiesce, drop leftover temps, then a final read.
                        (gen/nemesis (gen/once {:type :info :f :heal-restart}))
                        (gen/sleep 5)
                        (gen/clients (gen/once {:f :cleanup}))
                        (gen/clients (gen/once {:f :read})))
                       ;; un-fsynced: exercises semi-sync REPLICATION (the standby must
                       ;; replay the tail to keep acked-but-unflushed writes; the shared
                       ;; store does not have them). No-acked-loss for un-fsynced writes
                       ;; is Connected-only, so a SINGLE Connected kill-leader failover.
                       ;; Solo/kill-both would lose un-fsynced writes (POSIX-legal, not a
                       ;; bug). The survivor keeps serving, so no heal is needed.
                       (gen/phases
                        (gen/clients (gen/once {:f :read}))
                        (->> (gen/mix [(repeat {:f :add}) (repeat {:f :add})
                                       (repeat {:f :remove})])
                             ;; No rate limit: per-op latency bounds throughput.
                             (gen/nemesis (gen/phases (gen/sleep 25)
                                                      (gen/once {:type :info :f :kill-leader})))
                             (gen/time-limit (:time-limit opts)))
                        (gen/sleep 5)
                        (gen/clients (gen/once {:f :cleanup}))
                        (gen/clients (gen/once {:f :read}))))
          :nodes     ["n1"]
          :ssh       {:dummy? true}
          :pure-generators true}))

(def cli-opts
  "Extra CLI options. Path defaults read an env var first (set by run.sh locally
  or by CI), then fall back to portable values: a /tmp work dir, the repo's `ci`
  build, and minio/mc resolved on PATH."
  [[nil "--work-dir DIR" "Working directory for the cluster + mount"
    :default (or (System/getenv "ZEROFS_JEPSEN_WORK") "/tmp/zerofs-jepsen-ha")]
   [nil "--zerofs-bin PATH" "Path to the zerofs binary"
    :default (or (System/getenv "ZEROFS_BIN") "../../zerofs/target/ci/zerofs")]
   [nil "--minio-bin PATH" "Path to the minio binary"
    :default (or (System/getenv "MINIO_BIN") "minio")]
   [nil "--mc-bin PATH" "Path to the mc (minio client) binary"
    :default (or (System/getenv "MC_BIN") "mc")]
   [nil "--[no-]fsync" "fsync every add (default true). --no-fsync acks writes without flushing, so a single Connected kill-leader failover tests semi-sync replication of un-fsynced writes."
    :default true]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn ha-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
