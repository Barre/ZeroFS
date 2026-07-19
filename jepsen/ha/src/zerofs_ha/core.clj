(ns zerofs-ha.core
  "Jepsen HA test for ZeroFS: local MinIO + a ZeroFS leader/standby pair over it +
  a multi-target FUSE mount. Nemesis kills leader/standby/both/MinIO then heals.
  Workload is a grow-only set (add = create+fsync a uniquely-named file, fsync
  being a global durability barrier; read = ls); set-full checker asserts no
  acked add is lost. All-local: one logical node, dummy SSH, process management
  via start-stop-daemon + shell rather than jepsen.control. A separate
  --fsync-honesty scenario checks that a successful fsync never reports success for
  an un-fsync'd write lost to a both-nodes cold restart."
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
  (:import [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio.channels SocketChannel]
           [java.util BitSet])
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
  ;; The liveness endpoint can precede S3 readiness. Require alias setup,
  ;; idempotent creation, and a successful bucket stat before starting ZeroFS.
  (await-fn (fn []
              (sh-ok! (:mc c) "alias" "set" "j" (str "http://" (:minio-addr c))
                      (:access-key c) (:secret-key c))
              (sh-ok! (:mc c) "mb" "--ignore-existing" (str "j/" (:bucket c)))
              (sh-ok! (:mc c) "stat" (str "j/" (:bucket c))))
            {:retry-interval 200 :log-interval 5000 :log-message "Waiting for MinIO bucket"}))

(defn node-cfg-str
  ([c node-key role] (node-cfg-str c node-key role false))
  ([c node-key role force-recovery?]
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
          (if force-recovery?
            "peers = []\nforce_recovery = true\n\n"
            (str "peers = [\"127.0.0.1:" (:peer-port n) "\"]\n\n"))
          "[aws]\n"
          "access_key_id = \"" (:access-key c) "\"\n"
          "secret_access_key = \"" (:secret-key c) "\"\n"
          "endpoint = \"http://" (:minio-addr c) "\"\n"
          "region = \"us-east-1\"\n"
          "allow_http = \"true\"\n"
          "conditional_put = \"etag\"\n\n"
          "[telemetry]\nenabled = false\n"))))

(defn node-cfg-path [c node-key]
  (str (:work c) "/" (name node-key) ".toml"))

(defn start-node!
  ([c node-key role] (start-node! c node-key role false))
  ([c node-key role force-recovery?]
   (let [n    (get-in c [:nodes node-key])
         path (node-cfg-path c node-key)]
     (info "Starting ZeroFS node" node-key "as" role)
     (spit path (node-cfg-str c node-key role force-recovery?))
     (daemon-start! (:pid n) (:log n) {} (:zerofs c) ["run" "-c" path]))))

(defn node-9p-up? [c node-key]
  (try
    (with-open [channel (SocketChannel/open StandardProtocolFamily/UNIX)]
      (.connect channel
                (UnixDomainSocketAddress/of
                 (get-in c [:nodes node-key :ninep]))))
    true
    (catch Exception _ false)))

(defn standby-ready-count [c node-key]
  (try
    (let [marker (str "HA standby " (name node-key)
                      ": watching leader heartbeats")]
      (->> (slurp (get-in c [:nodes node-key :log]))
           (re-seq (re-pattern (java.util.regex.Pattern/quote marker)))
           count))
    (catch java.io.IOException _ 0)))

(defn await-standby-ready! [c node-key base]
  (await-fn
   (fn [] (or (> (standby-ready-count c node-key) base)
              (throw+ {:type ::standby-not-ready :node node-key})))
   {:retry-interval 200 :log-interval 5000 :timeout 120000
    :log-message (str "Waiting for standby " (name node-key))}))

(defn start-standby! [c node-key]
  (let [base (standby-ready-count c node-key)]
    (start-node! c node-key "standby")
    (await-standby-ready! c node-key base)))

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
        (start-standby! c :b)
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
  statfs/read see only real set files. Retries in a bounded loop, re-listing each
  round, since one best-effort pass can miss a temp a just-restarted node is still
  materializing or whose delete transiently failed."
  [dir]
  (loop [attempt 0]
    (let [temps (->> (file-seq (io/file dir))
                     (filter #(and (.isFile %) (.startsWith (.getName %) ".tmp-")))
                     vec)]
      (when (seq temps)
        (run! #(.delete %) temps)
        (when (< attempt 20)
          (Thread/sleep 100)
          (recur (inc attempt)))))))

(defn read-set
  "Integers currently present as files anywhere under dir."
  [dir]
  (into (sorted-set) (map second) (integer-files dir)))

(defn count-all-inodes
  "Every inode the tree holds: all directories + regular files, temps and the sync
  sentinel included. This is what the usage stats count, so comparing its growth to
  the statfs used-inode growth stays exact even when an interrupted-rename temp
  survives (a real inode `read-set`, being integer-names only, deliberately skips)."
  [dir]
  (count (file-seq (io/file dir))))

(defn non-integer-files
  "Names of regular files that aren't integer-named (leftover .tmp-* temps, the sync
  sentinel): surfaced on a statfs mismatch so the extra inode is identifiable."
  [dir]
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (remove #(try (Long/parseLong (.getName %)) (catch Exception _ nil)))
       (mapv #(.getName %))))

(defn statfs-used-inodes
  "Inodes the filesystem's usage stats account for, via statfs (`files - ffree`).
  ZeroFS derives `files`/`ffree` from its global usage stats + the inode-id
  allocator, so a takeover that regressed either (by replaying an already-flushed
  tail) under-reports here: an accounting leak the name-only set check can't see."
  [dir]
  (let [{:keys [out exit]} (sh! :stat :-f "--format=%c %d" (str dir))]
    (when-not (zero? exit)
      (throw (java.io.IOException. (str "statfs failed (" exit "): " out))))
    ;; `stat` may render a free-inode count >= 2^63 as a signed-negative string
    ;; (GNU coreutils does; uutils prints it unsigned). Parsing is to bigint, so the
    ;; text reads fine either way; mask both fields back to unsigned before
    ;; subtracting so the result is the true (small) used-inode count and fits a long.
    (let [u64 (fn [x] (if (neg? x) (+ x 18446744073709551616N) x))
          [total free] (->> (str/split (str/trim out) #"\s+") (mapv bigint))]
      (long (- (u64 total) (u64 free))))))

(def inode-accounting-timeout-ms 300000)

(defn await-inode-accounting!
  "Poll statfs after namespace mutations stop until its inode excess is gone.
  The expected count preserves the offset measured by the baseline read. Completed
  create results pin their inodes for 150 seconds, and unlink defers reclamation
  while a pin remains. The remaining timeout covers expiry processing and serial
  orphan reclamation. An undercount returns immediately for the statfs checker to
  report."
  [dir {:keys [used-inodes all-inodes]}]
  (let [expected-used (+ used-inodes (- (count-all-inodes dir) all-inodes))]
    (await-fn
     (fn []
       (let [used (statfs-used-inodes dir)]
         (if (<= used expected-used)
           used
           (throw+ {:type ::inode-reclamation-pending
                    :used-inodes used
                    :expected-used-inodes expected-used}))))
     {:retry-interval 1000
      :log-interval 10000
      :timeout inode-accounting-timeout-ms
      :log-message "Waiting for inode reclamation"})))

(defn corrupt-files
  "Integer-named files whose content isn't \"<name>\\n\", e.g. two names that
  ended up sharing an inode after an inode-id allocator regression. Returns names."
  [dir]
  (->> (integer-files dir)
       (keep (fn [[f n]]
               (when (not= (str n "\n") (try (slurp f) (catch Exception _ ::read-error)))
                 n)))
       (into (sorted-set))))

(defn classify-add-error
  "Classify add-path I/O failures as indeterminate. The target may have been
  created or renamed before an exception, including an fsync failure."
  [e]
  (let [msg (str (.getMessage e))]
    (cond
      (instance? java.nio.file.FileAlreadyExistsException e)
      {:type :info :error :retried-create}
      (instance? java.nio.file.NoSuchFileException e)
      {:type :info :error :not-durable-here}
      (re-find #"(?i)file exists" msg)
      {:type :info :error :retried-create}
      (re-find #"(?i)no such file|not found" msg)
      {:type :info :error :not-durable-here}
      :else {:type :info :error msg})))

;; `counter` assigns globally-unique add values. `live` maps each worker to values
;; it added and has not removed, preventing concurrent add/remove of one value.
;; `inode-baseline` is the first read's statfs and census pair. All three are shared
;; because open! returns this record for every worker.
(defrecord SetClient [dir fsync? counter live inode-baseline]
  client/Client
  (open! [this _test _node] this)
  (setup! [_ _test]
    ;; Pre-create the subdirs + the fsync sentinel BEFORE the baseline read, so the
    ;; statfs baseline already counts them and final-minus-baseline growth is files.
    (dotimes [i ndirs] (.mkdirs (io/file dir (str "d" i))))
    (.createNewFile (io/file dir ".sync")))
  (invoke! [_ _test op]
    ;; Reads happen only at the baseline (empty fs) and the final read, both outside
    ;; any fault window, so they get NO timeout: the final read must enumerate the
    ;; whole set, however large it grew. A hang here means a genuinely wedged fs
    ;; (caught by the outer job timeout), not data loss. Writes stay bounded: during
    ;; an outage a FUSE op blocks until recovery and would wedge the run -> :info.
    (if (= :read (:f op))
      ;; read carries several signals on one op so the set checker only sees :add/
      ;; :remove/:read: :value = the live name set; :used-inodes = statfs accounting;
      ;; :all-inodes = the real inode census (for the statfs check, robust to a leftover
      ;; temp); :non-integer = those temps/sentinel by name (diagnostic); :corrupt =
      ;; files whose content != name.
      (try (let [used-inodes (statfs-used-inodes dir)
                 all-inodes  (count-all-inodes dir)
                 value       (read-set dir)
                 non-integer (non-integer-files dir)
                 corrupt     (corrupt-files dir)]
             (compare-and-set! inode-baseline nil
                               {:used-inodes used-inodes
                                :all-inodes all-inodes})
             (assoc op :type :ok
                    :value value
                    :used-inodes used-inodes
                    :all-inodes all-inodes
                    :non-integer non-integer
                    :corrupt corrupt))
           (catch java.io.IOException e
             (assoc op :type :fail :error (.getMessage e))))
      (case (:f op)
        ;; Assign a unique value, record it under this worker, then create it.
        :add (let [v (swap! counter inc)
                   f (io/file (subdir dir v) (str v))]
               (swap! live update (:process op) (fnil conj #{}) v)
               (util/timeout 30000 (assoc op :type :info :value v :error :timeout)
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
                     (merge op {:value v} (classify-add-error e))))))
        ;; Remove one of THIS worker's live values (none queued -> nothing to do).
        :remove (if-let [v (first (get @live (:process op)))]
                  (let [f (io/file (subdir dir v) (str v))]
                    (swap! live update (:process op) disj v)
                    (util/timeout 30000 (assoc op :type :info :value v :error :timeout)
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
                          (assoc op :type :info :value v :error (.getMessage e))))))
                  (assoc op :type :fail :error :nothing-to-remove))
        ;; An acknowledged write WITHOUT fsync: it lives in the leader memtable +
        ;; the standby tail, not yet on the object store. :ok means acked (and
        ;; readable here now), NOT durable.
        :write (let [v (swap! counter inc)
                     f (io/file (subdir dir v) (str v))]
                 (util/timeout 30000 (assoc op :type :info :value v :error :timeout)
                   (try
                     (add-file! dir v)
                     (if (= (str v "\n") (slurp f))
                       (assoc op :type :ok :value v)
                       (assoc op :type :info :value v :error :content-not-here))
                     (catch java.io.IOException e
                       (assoc op :type :info :value v :error (str (.getMessage e)))))))
        ;; A global fsync durability barrier. :ok = the barrier held; an error
        ;; (after the fix, a fsync that spans a recovery which dropped this
        ;; client's un-fsync'd writes) surfaces here as :fail.
        :fsync (util/timeout 30000 (assoc op :type :info :error :timeout)
                 (try (fsync-sentinel! dir) (assoc op :type :ok)
                      (catch java.io.IOException e
                        (assoc op :type :fail :error (str (.getMessage e))))))
        ;; Remove interrupted-rename temps, then wait for replay-protected orphans.
        :cleanup (util/timeout (+ inode-accounting-timeout-ms 30000)
                   (assoc op :type :info :error :timeout)
                   (try
                     (cleanup-temps! dir)
                     (if-let [baseline @inode-baseline]
                       (try+
                         (await-inode-accounting! dir baseline)
                         (assoc op :type :ok)
                         (catch [:type :timeout] _
                           (assoc op :type :info :error :inode-accounting-timeout)))
                       (assoc op :type :fail :error :missing-inode-baseline))
                     (catch java.io.IOException e
                       (assoc op :type :fail :error (.getMessage e))))))))
  (teardown! [_ _test])
  (close! [_ _test]))

;; Per-fid durability client. fsync is per-fd POSIX (fsync on a fid verifies only
;; that fid's writes, not a global barrier), so durability honesty is tested by
;; holding files OPEN and fsyncing each by name. Ops carry :file so the checker
;; matches a write to the fsync that covers it. `handles` is a shared
;; name->RandomAccessFile map (the deterministic scenarios run one logical thread).

(defn dc-file [dir f] (io/file dir f))

(defrecord DurabilityClient [dir handles counter]
  client/Client
  (open! [this _test _node] this)
  (setup! [_this _test] (.mkdirs (io/file dir)))
  (invoke! [_ _test op]
    ;; A handle is keyed by (:as op) when given, else by (:file op), so the SAME file
    ;; can be opened on TWO handles (:as "a"/"b"). The checker matches by the
    ;; underlying :file, so a write on one handle and a fsync on another (same file)
    ;; are paired.
    (let [hkey (or (:as op) (:file op) (:dir op))]
      (util/timeout 30000 (assoc op :type :info :error :timeout)
        (case (:f op)
          ;; Open (create) a file, fsync to make its existence durable, so only the
          ;; later un-fsync'd appends, not the file itself, are at risk.
          :open (let [raf (java.io.RandomAccessFile. (dc-file dir (:file op)) "rw")]
                  (.sync (.getFD raf))
                  (swap! handles assoc hkey {:raf raf :file (:file op)})
                  (assoc op :type :ok))
          ;; Append a unique value via this handle; NOT fsync'd (acked, not durable).
          ;; `locking` serializes concurrent appends so two writes never race on
          ;; seek+write to the same offset (test artifact, not durability).
          :write (let [v (swap! counter inc)
                       {:keys [raf file]} (get @handles hkey)]
                   (locking raf
                     (.seek raf (.length raf))
                     (.write raf (.getBytes (str v "\n"))))
                   (assoc op :type :ok :value v :file file))
          ;; ftruncate(fd) to a size: metadata-only change on the open fd (no data
          ;; write). Via FUSE this is a setattr, which the mount lands on the per-inode
          ;; fid, NOT this open handle.
          :truncate (let [{:keys [raf file]} (get @handles hkey)]
                      (locking raf (.setLength raf (long (:to op))))
                      (assoc op :type :ok :value (:to op) :file file))
          ;; fsync this handle's fd. :fail surfaces an ESTALE honestly. The result
          ;; carries the underlying :file so the checker pairs it with writes to that
          ;; file made through ANY handle.
          :fsync (let [{:keys [raf file]} (get @handles hkey)]
                   (try (.sync (.getFD raf)) (assoc op :type :ok :file file)
                        (catch java.io.IOException e
                          (assoc op :type :fail :file file :error (str (.getMessage e))))))
          ;; Final state per distinct file: {file -> {:values #{values} :size bytes}}.
          :read (assoc op :type :ok
                       :value (into {}
                                    (for [f (distinct (keep :file (vals @handles)))]
                                      [f {:size (.length (dc-file dir f))
                                          :values (with-open [r (io/reader (dc-file dir f))]
                                                    (into (sorted-set)
                                                          (keep #(try (Long/parseLong %)
                                                                      (catch Exception _ nil))
                                                                (line-seq r))))}])))

          ;; Open a directory, fsync it (a force on a dir channel flushes the db, so the
          ;; directory exists durably and only its later un-fsync'd entries are at risk),
          ;; and keep the channel keyed by the directory name.
          :opendir (let [f (dc-file dir (:dir op))]
                     (.mkdirs f)
                     (let [ch (java.nio.channels.FileChannel/open
                               (.toPath f)
                               (into-array java.nio.file.OpenOption
                                           [java.nio.file.StandardOpenOption/READ]))]
                       (.force ch true)
                       (swap! handles assoc hkey {:dirchan ch :dirname (:dir op)})
                       (assoc op :type :ok)))
          ;; Create a subdirectory (un-fsync'd). The entry is durable only once its
          ;; parent is fsyncdir'd. :dir-facts says what the parent's listing must show.
          :mkdir (do (.mkdir (io/file (dc-file dir (:in op)) (:name op)))
                     (assoc op :type :ok
                            :dir-facts [{:dir (:in op) :name (:name op) :present true}]))
          ;; Create an empty file in a directory (un-fsync'd); used to set up a rename.
          :mkfile (do (.createNewFile (io/file (dc-file dir (:in op)) (:name op)))
                      (assoc op :type :ok))
          ;; Move an entry between directories (un-fsync'd). The source loses the entry
          ;; and the dest gains it, so fsyncdir of EITHER must account for the change.
          :rename (do (java.nio.file.Files/move
                       (.toPath (io/file (dc-file dir (:from-dir op)) (:from-name op)))
                       (.toPath (io/file (dc-file dir (:to-dir op)) (:to-name op)))
                       (into-array java.nio.file.CopyOption []))
                      (assoc op :type :ok
                             :dir-facts [{:dir (:from-dir op) :name (:from-name op) :present false}
                                         {:dir (:to-dir op) :name (:to-name op) :present true}]))
          ;; fsync a directory. :fail surfaces an ESTALE honestly. Carries :dir so the
          ;; checker pairs it with entry changes to that directory.
          :fsyncdir (let [{:keys [dirchan dirname]} (get @handles (:dir op))]
                      (try (.force dirchan true) (assoc op :type :ok :dir dirname)
                           (catch java.io.IOException e
                             (assoc op :type :fail :dir dirname :error (str (.getMessage e))))))
          ;; Final entry set per open directory: {dir -> #{entry names}}.
          :readdir (assoc op :type :ok
                          :value (into {}
                                       (for [{:keys [dirname]} (vals @handles) :when dirname]
                                         [dirname (into (sorted-set)
                                                        (map #(.getName %)
                                                             (or (.listFiles (dc-file dir dirname))
                                                                 (into-array java.io.File []))))])))))))
  (teardown! [_ _test])
  (close! [_ _test]))

;; Nemesis: process loss, object-store pauses, replication partitions, blocked
;; survivor restart, forced recovery, and cluster repair.

(defn heal-restart!
  "Clean full restart to canonical leader=:a, standby=:b. Works from any state
  (incl. kill-both); the multi-target client re-routes."
  [c]
  (when-not (minio-up? c) (start-minio! c))
  (kill-pid! (node-pid c :a))
  (kill-pid! (node-pid c :b))
  (Thread/sleep 1000)
  (let [standby-base (standby-ready-count c :b)]
    ;; Start both receivers before waiting; the recorded latest writer may block
    ;; until its peer answers Hello.
    (start-node! c :b "standby")
    (start-node! c :a "leader")
    (await-fn (fn [] (or (node-9p-up? c :a) (throw+ {:type ::leader-down})))
              {:retry-interval 200 :log-interval 5000 :log-message "heal: waiting for leader"})
    (await-standby-ready! c :b standby-base))
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
          (start-standby! c dead)
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
                 :resume-minio (do (resume-pid! (:minio-pid c))
                                   ;; Restart ZeroFS after store recovery.
                                   (Thread/sleep 500)
                                   (heal-restart! c)
                                   :resumed-minio)
                 :await-serving (do (await-fn
                                     (fn [] (or (node-9p-up? c :a)
                                                (node-9p-up? c :b)
                                                (throw+ {:type ::no-serving-node})))
                                     {:retry-interval 500 :log-interval 5000 :timeout 40000
                                      :log-message "failover: waiting for an authoritative listener"})
                                    :serving)
                 ;; Hold the leader through failure detection and claim grace.
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
                                        (Thread/sleep 10000) ; allow stale-writer detection
                                        (info "resume: stale leader" p
                                              (if (proc-alive? (node-pid c p))
                                                "still up (should be refusing)" "self-fenced (exited)"))
                                        (kill-pid! (node-pid c p))   ; discard the fenced zombie
                                        (start-standby! c p)
                                        (await-fn (fn [] (or (mounted? c) (throw+ {:type ::not-mounted})))
                                                  {:retry-interval 500 :log-interval 5000
                                                   :log-message "resume: waiting for mount"})
                                        (swap! cluster-roles assoc p :standby)
                                        (str "resumed-" (name p)))
                                    (do (heal-restart! c) "resume-fell-back-to-restart")))
                 ;; Restart before peer-failure detection. Hello must defer to the
                 ;; active standby and preserve its tail.
                 :bounce-leader (let [n (leader-node) s (standby-node)]
                                  (kill-pid! (node-pid c n))
                                  (Thread/sleep 500)
                                  (start-node! c n "leader")
                                  (when s (swap! cluster-roles assoc s :leader))
                                  (swap! cluster-roles assoc n :standby)
                                  (str "bounced-leader-" (name n)))
                 :blocked-restart (let [n (leader-node) s (standby-node)]
                                    (sh! :rm :-f (get-in c [:nodes s :ninep]))
                                    (kill-pid! (node-pid c n))
                                    (swap! cluster-roles assoc n :dead)
                                    (swap! cluster-roles assoc s :leader)
                                    (await-fn (fn [] (or (node-9p-up? c s)
                                                         (throw+ {:type ::no-promote})))
                                              {:retry-interval 500 :log-interval 5000 :timeout 40000
                                               :log-message "recovery: waiting for standby to promote"})
                                    (Thread/sleep 3000)
                                    (kill-pid! (node-pid c s))
                                    (sh! :rm :-f (get-in c [:nodes s :ninep]))
                                    (Thread/sleep 500)
                                    (start-node! c s "standby")
                                    (Thread/sleep 5000)
                                    (when-not (proc-alive? (node-pid c s))
                                      (throw+ {:type ::blocked-survivor-exited}))
                                    (when (node-9p-up? c s)
                                      (throw+ {:type ::unsafe-retake}))
                                    (kill-pid! (node-pid c s))
                                    (sh! :rm :-f (get-in c [:nodes s :ninep]))
                                    (Thread/sleep 500)
                                    (start-node! c s "leader" true)
                                    (await-fn (fn [] (or (node-9p-up? c s)
                                                         (throw+ {:type ::no-forced-recovery})))
                                              {:retry-interval 500 :log-interval 5000 :timeout 60000
                                               :log-message "recovery: waiting for forced startup"})
                                    ;; Do not leave force_recovery enabled after startup.
                                    (spit (node-cfg-path c s) (node-cfg-str c s "leader"))
                                    (str "blocked-then-recovered-" (name s)))
                 :heal-rejoin  (do (heal-rejoin! c) :healed-rejoin)
                 ;; Cut replication traffic while retaining client and store access.
                 ;; Marker validation retires the old server; the writer epoch
                 ;; remains the durable-write fence.
                 :partition  (let [r @relays]
                               ((:cut! (:to-b r)))
                               ((:cut! (:to-a r)))
                               :partitioned)
                 :heal-partition (let [r @relays]
                                   ((:heal! (:to-b r)))
                                   ((:heal! (:to-a r)))
                                   (heal-restart! c) ; restore canonical topology
                                   :healed-partition)
                 :heal-restart (do (heal-restart! c) :healed-restart)))))
    (teardown! [_ _test])))

;; Each fault has a paired recovery. MinIO is paused because restarting the
;; single-drive test instance can lose acknowledged, un-fsynced PUTs.
(def scenarios
  [{:fault :kill-leader  :heal :heal-rejoin}
   {:fault :kill-standby :heal :heal-rejoin}
   {:fault :kill-leader  :heal :heal-restart}
   {:fault :kill-standby :heal :heal-restart}
   {:fault :kill-both    :heal :heal-restart}
   {:fault :pause-minio  :heal :resume-minio}
   ;; Hold through failure detection and the pre-open claim grace.
   {:fault :pause-leader :heal :resume-leader :hold 32}
   ;; Hello makes a restarted leader defer to an active standby.
   {:fault :bounce-leader    :heal :heal-restart :hold 13}
   ;; A recorded writer blocks on a silent peer until explicit recovery.
   {:fault :blocked-restart  :heal :heal-restart :hold 6}
   ;; Peer-only partition; both nodes retain object-store access.
   {:fault :partition       :heal :heal-partition :hold 17}])

(defn nemesis-gen
  "Cycle fault/recovery pairs. The default 20-second hold exceeds failure
  detection and the pre-open claim grace."
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
            ^BitSet seen       (BitSet.)
            ^BitSet present    (BitSet.)
            ^BitSet unknown    (BitSet.)
            ^BitSet final-bits (BitSet.)
            _         (doseq [{:keys [type f value]} history
                              :when (and (#{:ok :info :fail} type)
                                         (#{:add :remove} f)
                                         (some? value))]
                        (let [i (int value)]
                          (.set seen i)
                          (case type
                            :ok (do (.clear unknown i)
                                    (if (= :add f)
                                      (.set present i)
                                      (.clear present i)))
                            :info (do (.clear present i)
                                      (.set unknown i))
                            nil)))
            _         (doseq [v final-set]
                        (.set final-bits (int v)))
            ^BitSet lost (.clone present)
            _         (.andNot lost final-bits)
            ^BitSet resurrected (.clone final-bits)
            _         (.and resurrected seen)
            _         (.andNot resurrected present)
            _         (.andNot resurrected unknown)]
        {:valid?            (and (.isEmpty lost) (.isEmpty resurrected))
         :present-required  (.cardinality present)
         :final-set-size    (count final-set)
         :lost-count        (.cardinality lost)
         :lost              (vec (.toArray (.limit (.stream lost) 50)))
         :resurrected-count (.cardinality resurrected)
         :resurrected       (vec (.toArray (.limit (.stream resurrected) 50)))}))))

(defn statfs-checker
  "The growth in statfs-reported used inodes (a baseline read on the empty fs vs the
  final read, after temp cleanup) must equal the growth in the tree's actual inode
  census: every created file/dir is one inode, adds increment and removes decrement,
  so the two move together. A takeover that regressed the usage stats / inode-id
  allocator, or mishandled a delete's decrement, breaks the equality even when `ls`
  (the set checker) still passes.

  The census (not the integer-set size) is the reference on purpose: a fault can leave
  an interrupted-rename `.tmp-` behind, which is a real inode statfs correctly counts
  but `read-set` (integer names only) skips. Counting against the set size would then
  false-fail on a perfectly consistent fs; counting against the census does not, while
  still catching a genuine stats/allocator drift (used-inodes != census)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [reads (->> history
                       (filter #(and (= :ok (:type %)) (= :read (:f %))))
                       vec)
            baseline    (some-> (first reads) :used-inodes)
            final       (some-> (last reads) :used-inodes)
            census-base (some-> (first reads) :all-inodes)
            census-fin  (some-> (last reads) :all-inodes)
            n           (some-> (last reads) :value count)]
        (if (or (nil? baseline) (nil? final) (nil? census-base) (nil? census-fin)
                (< (count reads) 2))
          {:valid? :unknown
           :reason "need a baseline read (empty fs) and a final read"}
          {:valid?               (= (- final baseline) (- census-fin census-base))
           :baseline-used        baseline
           :final-used           final
           :inodes-grew          (- final baseline)
           :census-grew          (- census-fin census-base)
           :files-in-set         n
           :leftover-non-integer (some-> (last reads) :non-integer)})))))

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

(defn fsync-honesty-checker
  "Per-fid fsync durability contract: for each file, if a write to it is acknowledged
  and the FIRST fsync of THAT file after it returns :ok, that write must be present
  in the file after recovery. fsync is per-fd POSIX, so an fsync of file A says
  nothing about file B; matching the fsync to the write's own file keeps an fsync of
  A from counting B's lost write as required. A FAILED fsync signals the loss
  honestly. `clean-fsync-ok?` (the last fsync runs in a healthy window, so it must be
  :ok) rejects a checker that passes by treating every fsync as a failure. The final
  read returns {file -> #{values}}."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [indexed (vec (map-indexed (fn [i op] (assoc op ::i i)) history))
            final   (or (->> indexed
                             (filter #(and (= :ok (:type %)) (= :read (:f %))))
                             last :value)
                        {})
            ;; Pair each fsync's :invoke with its completion, carrying its :file.
            fsyncs  (->> indexed
                         (reduce (fn [{:keys [pending done]} op]
                                   (if (= :fsync (:f op))
                                     (case (:type op)
                                       ;; Pair invoke->completion by process. Take :file
                                       ;; from the COMPLETION: an `:as`-keyed fsync invoke
                                       ;; carries no :file (the client resolves it from the
                                       ;; handle and sets it on the reply).
                                       :invoke {:pending (assoc pending (:process op) (::i op))
                                                :done done}
                                       (:ok :fail :info)
                                       {:pending (dissoc pending (:process op))
                                        :done (if-let [iv (get pending (:process op))]
                                                (conj done {:invoke-i iv :file (:file op)
                                                            :result (:type op)})
                                                done)}
                                       {:pending pending :done done})
                                     {:pending pending :done done}))
                                 {:pending {} :done []})
                         :done
                         (sort-by :invoke-i)
                         vec)
            first-fsync-after (fn [iw f]
                                (first (filter #(and (> (:invoke-i %) iw) (= f (:file %))) fsyncs)))
            writes  (->> indexed
                         (filter #(and (= :ok (:type %)) (= :write (:f %)) (some? (:value %)))))
            required (->> writes
                          (filter (fn [w] (= :ok (:result (first-fsync-after (::i w) (:file w))))))
                          (map (fn [w] [(:file w) (:value w)]))
                          (into (sorted-set)))
            lost    (->> required
                         (remove (fn [[f v]] (contains? (get-in final [f :values] #{}) v)))
                         vec)
            clean-fsync-ok? (= :ok (:result (last fsyncs)))]
        {:valid?           (and (empty? lost) clean-fsync-ok?)
         :required-present (count required)
         :lost-count       (count lost)
         :lost             (vec (take 50 lost))
         :fsyncs           (mapv :result fsyncs)
         :clean-fsync-ok?  clean-fsync-ok?}))))

(defn metadata-honesty-checker
  "Per-fid fsync honesty for metadata-only changes (ftruncate). If a truncate of a
  file to size N is acknowledged and the FIRST fsync of THAT file after it returns
  :ok, the file must be at size N after recovery. Under FUSE the setattr lands on the
  per-inode fid while fsync rides the open handle, so an fsync that does not aggregate
  every fid bound to the inode could report :ok over a lost truncate.
  `clean-fsync-ok?` rejects a checker that passes by failing every fsync. The final
  read gives {file -> {:size N ...}}."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [indexed (vec (map-indexed (fn [i op] (assoc op ::i i)) history))
            final   (or (->> indexed
                             (filter #(and (= :ok (:type %)) (= :read (:f %))))
                             last :value)
                        {})
            fsyncs  (->> indexed
                         (reduce (fn [{:keys [pending done]} op]
                                   (if (= :fsync (:f op))
                                     (case (:type op)
                                       ;; Pair invoke->completion by process. Take :file
                                       ;; from the COMPLETION: an `:as`-keyed fsync invoke
                                       ;; carries no :file (the client resolves it from the
                                       ;; handle and sets it on the reply).
                                       :invoke {:pending (assoc pending (:process op) (::i op))
                                                :done done}
                                       (:ok :fail :info)
                                       {:pending (dissoc pending (:process op))
                                        :done (if-let [iv (get pending (:process op))]
                                                (conj done {:invoke-i iv :file (:file op)
                                                            :result (:type op)})
                                                done)}
                                       {:pending pending :done done})
                                     {:pending pending :done done}))
                                 {:pending {} :done []})
                         :done
                         (sort-by :invoke-i)
                         vec)
            first-fsync-after (fn [iw f]
                                (first (filter #(and (> (:invoke-i %) iw) (= f (:file %))) fsyncs)))
            truncates (->> indexed
                           (filter #(and (= :ok (:type %)) (= :truncate (:f %)) (some? (:value %)))))
            required  (->> truncates
                           (filter (fn [t] (= :ok (:result (first-fsync-after (::i t) (:file t))))))
                           (map (fn [t] [(:file t) (:value t)])))
            lost      (->> required
                           (remove (fn [[f sz]] (= sz (get-in final [f :size]))))
                           vec)
            clean-fsync-ok? (= :ok (:result (last fsyncs)))]
        {:valid?           (and (empty? lost) clean-fsync-ok?)
         :required-present (count required)
         :lost-count       (count lost)
         :lost             (vec (take 50 lost))
         :fsyncs           (mapv :result fsyncs)
         :clean-fsync-ok?  clean-fsync-ok?}))))

(defn dirent-honesty-checker
  "Per-fid fsync honesty for DIRECTORY ENTRIES (mkdir, rename). Each directory op
  records `:dir-facts`, one `{:dir :name :present}` per directory it changes (a mkdir
  adds an entry; a rename removes from the source and adds to the dest). If the FIRST
  fsyncdir of that directory after the op returns :ok, the fact must hold in the final
  listing: a `:present true` entry must be present, a `:present false` entry absent.
  Catches an fsync of a directory that OKs over a lost entry change. `clean-fsyncdir-ok?`
  guards against a fix that just fails every fsyncdir. The final read is `:readdir`,
  giving {dir -> #{entry names}}."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [indexed (vec (map-indexed (fn [i op] (assoc op ::i i)) history))
            final   (or (->> indexed
                             (filter #(and (= :ok (:type %)) (= :readdir (:f %))))
                             last :value)
                        {})
            fsyncdirs (->> indexed
                           (reduce (fn [{:keys [pending done]} op]
                                     (if (= :fsyncdir (:f op))
                                       (case (:type op)
                                         :invoke {:pending (assoc pending (:process op) (::i op))
                                                  :done done}
                                         (:ok :fail :info)
                                         {:pending (dissoc pending (:process op))
                                          :done (if-let [iv (get pending (:process op))]
                                                  (conj done {:invoke-i iv :dir (:dir op)
                                                              :result (:type op)})
                                                  done)}
                                         {:pending pending :done done})
                                       {:pending pending :done done}))
                                   {:pending {} :done []})
                           :done
                           (sort-by :invoke-i)
                           vec)
            first-fsyncdir-after (fn [iw d]
                                   (first (filter #(and (> (:invoke-i %) iw) (= d (:dir %)))
                                                  fsyncdirs)))
            facts   (->> indexed
                         (filter #(and (= :ok (:type %)) (seq (:dir-facts %))))
                         (mapcat (fn [op] (map #(assoc % ::i (::i op)) (:dir-facts op)))))
            required (->> facts
                          (filter (fn [f] (= :ok (:result (first-fsyncdir-after (::i f) (:dir f)))))))
            violated (->> required
                          (remove (fn [f]
                                    (= (:present f)
                                       (contains? (get final (:dir f) #{}) (:name f)))))
                          vec)
            clean-fsyncdir-ok? (= :ok (:result (last fsyncdirs)))]
        {:valid?            (and (empty? violated) clean-fsyncdir-ok?)
         :required-count    (count required)
         :violated-count    (count violated)
         :violated          (vec (take 50 violated))
         :fsyncdirs         (mapv :result fsyncdirs)
         :clean-fsyncdir-ok? clean-fsyncdir-ok?}))))

(defn metadata-gen
  "fsync-honesty for a metadata-only change. Open a file, give it durable data, then
  ftruncate(fd) it un-fsync'd: under FUSE a setattr the mount lands on the per-inode
  fid, not the open handle the app fsyncs. A cold double-restart loses the truncate,
  so a verified fsync(fd) of that file MUST fail rather than report success. A clean
  truncate+fsync in a healthy window then must be :ok."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "m"}))
   ;; Durable data so the later truncate is a real metadata change with a known size.
   (gen/clients (gen/time-limit 3 (repeat {:f :write :file "m"})))
   (gen/clients (gen/once {:f :fsync :file "m"}))
   (gen/clients (gen/once {:f :read}))                    ; baseline
   ;; Un-fsync'd ftruncate(fd) to 0: a setattr on the per-inode fid, not the open handle.
   (gen/clients (gen/once {:f :truncate :file "m" :to 0}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :kill-both}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart}))
   (gen/sleep 5)
   ;; fsync(fd) of the file whose un-fsync'd truncate was lost: must FAIL.
   (gen/clients (gen/once {:f :fsync :file "m"}))
   (gen/sleep 1)
   ;; A clean truncate + fsync in a healthy window: must be :ok (final size = 7).
   (gen/clients (gen/once {:f :truncate :file "m" :to 7}))
   (gen/clients (gen/once {:f :fsync :file "m"}))
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn multihandle-gen
  "fsync-honesty across multiple open handles of one inode. Open the SAME file on two
  handles a and b, write un-fsync'd through b, cold double-restart loses it, then
  fsync through the OTHER handle a: it MUST fail. POSIX fsync(fd) persists the whole
  file regardless of which fd wrote it, so a verified fsync must aggregate every open
  handle of the inode and return ESTALE for b's lost write rather than report success.
  Writes/fsyncs pair by the underlying file, so the value checker applies."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "h" :as "ha"}))
   (gen/clients (gen/once {:f :open :file "h" :as "hb"}))
   (gen/clients (gen/once {:f :read}))                    ; baseline
   ;; un-fsync'd write through handle b
   (gen/clients (gen/once {:f :write :as "hb"}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :kill-both}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart}))
   (gen/sleep 5)
   ;; fsync through handle a (which never wrote): must FAIL over b's lost write.
   (gen/clients (gen/once {:f :fsync :as "ha"}))
   (gen/sleep 1)
   ;; Guard: a clean write through b + fsync through a, must be :ok.
   (gen/clients (gen/once {:f :write :as "hb"}))
   (gen/clients (gen/once {:f :fsync :as "ha"}))
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn dirent-gen
  "fsync-honesty for a DIRECTORY ENTRY. Open a directory, create a subdirectory in it
  un-fsync'd, cold double-restart loses the entry, then fsyncdir of that directory MUST
  fail. A clean mkdir+fsyncdir then must be :ok and present."
  []
  (gen/phases
   (gen/clients (gen/once {:f :opendir :dir "d"}))
   (gen/clients (gen/once {:f :readdir}))                 ; baseline
   (gen/clients (gen/once {:f :mkdir :in "d" :name "x"})) ; un-fsync'd entry
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :kill-both}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart}))
   (gen/sleep 5)
   (gen/clients (gen/once {:f :fsyncdir :dir "d"}))       ; must FAIL (x was lost)
   (gen/sleep 1)
   (gen/clients (gen/once {:f :mkdir :in "d" :name "y"})) ; clean entry
   (gen/clients (gen/once {:f :fsyncdir :dir "d"}))       ; must :ok, y present
   (gen/sleep 2)
   (gen/clients (gen/once {:f :readdir}))))

(defn rename-gen
  "fsync-honesty for a CROSS-DIRECTORY rename. Create a file in src durably, move it to
  dst un-fsync'd, cold double-restart loses the move (the file reappears in src), then
  fsyncdir of the SOURCE MUST fail: the rename's source-side removal is attributed to
  the source directory, not only the dest. A clean change to src then must be :ok."
  []
  (gen/phases
   (gen/clients (gen/once {:f :opendir :dir "src"}))
   (gen/clients (gen/once {:f :opendir :dir "dst"}))
   (gen/clients (gen/once {:f :mkfile :in "src" :name "a"}))
   (gen/clients (gen/once {:f :fsyncdir :dir "src"}))     ; make src/a durable
   (gen/clients (gen/once {:f :readdir}))                 ; baseline
   (gen/clients (gen/once {:f :rename :from-dir "src" :from-name "a"
                           :to-dir "dst" :to-name "b"}))  ; un-fsync'd
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :kill-both}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart}))
   (gen/sleep 5)
   (gen/clients (gen/once {:f :fsyncdir :dir "src"}))     ; must FAIL (a is back in src)
   (gen/sleep 1)
   (gen/clients (gen/once {:f :mkdir :in "src" :name "c"})) ; a clean tracked entry on src
   (gen/clients (gen/once {:f :fsyncdir :dir "src"}))     ; must :ok
   (gen/sleep 2)
   (gen/clients (gen/once {:f :readdir}))))

(defn metadata-transparency-gen
  "TRANSPARENCY for a METADATA change. ftruncate(fd) un-fsync'd on a Connected leader
  (shipped to the standby), then a CLEAN kill-leader. The standby replays the tail so
  the truncate SURVIVES, and a fsync of the file must SUCCEED with the truncated size
  intact. Verifies tail replay carries setattr, so the kept token stays honest for
  metadata, not only data."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "m"}))
   (gen/clients (gen/time-limit 3 (repeat {:f :write :file "m"})))
   (gen/clients (gen/once {:f :fsync :file "m"}))         ; durable baseline
   (gen/clients (gen/once {:f :read}))
   (gen/clients (gen/once {:f :truncate :file "m" :to 7})) ; un-fsync'd, shipped Connected
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :kill-leader})) ; clean failover, standby replays the truncate
   (gen/nemesis (gen/once {:type :info :f :await-serving}))
   (gen/clients (gen/once {:f :fsync :file "m"}))         ; must :ok (transparent), size stays 7
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn multifailover-gen
  "A write survives a TRANSPARENT failover, then a later cold restart loses it. The
  write is shipped Connected and carried (token kept) across a clean kill-leader, so it
  lives on the promoted leader's memtable; a subsequent kill-both + cold restart
  regenerates the token, so a fsync over the carried-then-lost write MUST fail. Verifies
  a kept token is still invalidated by a later lineage break (cross-term)."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "h"}))
   (gen/clients (gen/time-limit 4 (repeat {:f :write :file "h"}))) ; un-fsync'd, shipped
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :kill-leader})) ; failover 1: standby promotes, keeps token, replays
   (gen/nemesis (gen/once {:type :info :f :await-serving}))
   (gen/nemesis (gen/once {:type :info :f :kill-both}))   ; failover 2: the promoted leader also dies
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart})) ; cold restart regenerates the token
   (gen/sleep 5)
   (gen/clients (gen/once {:f :fsync :file "h"}))         ; must FAIL (the carried writes are gone)
   (gen/sleep 1)
   (gen/clients (gen/once {:f :write :file "h"}))
   (gen/clients (gen/once {:f :fsync :file "h"}))         ; guard :ok
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn honesty-gen
  "fsync-honesty (per-fid). Open a file (existence made durable), write a burst of
  acknowledged-but-un-fsync'd values to it, kill BOTH nodes + cold-restart (no tail,
  so the un-fsync'd writes are gone), then fsync THAT file: it must FAIL, the writes
  are lost. A clean write+fsync to it in a healthy window must be :ok, so a verified
  fsync is not simply failing every call."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "h"}))          ; create + fsync: existence durable
   (gen/clients (gen/once {:f :read}))                    ; baseline
   ;; acknowledged, un-fsync'd appends (only in memtable + standby tail)
   (gen/clients (gen/time-limit 6 (repeat {:f :write :file "h"})))
   (gen/sleep 1)
   ;; Cold double restart: both die, both reopen with no tail; the appends are gone.
   (gen/nemesis (gen/once {:type :info :f :kill-both}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart}))
   (gen/sleep 5)
   ;; Post-recovery fsync of the file: must FAIL (its un-fsync'd writes are gone).
   (gen/clients (gen/once {:f :fsync :file "h"}))
   (gen/sleep 1)
   ;; A clean write + fsync in a healthy window: must be :ok.
   (gen/clients (gen/once {:f :write :file "h"}))
   (gen/clients (gen/once {:f :fsync :file "h"}))
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn honesty-partition-gen
  "fsync-honesty under a partition (per-fid, the Solo-at-takeover case). Open a file,
  cut leader<->standby so the leader goes Solo (its ships fail); un-fsync'd writes to
  the file land on it but are NEVER shipped to the standby. The standby promotes +
  fences the old leader; the client re-routes, and a fsync of that file on the new
  leader MUST fail: it never received the Solo writes. The Solo term regenerates the
  lineage token, so the Solo writes' token mismatches and the fsync fails honestly."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "h"}))
   (gen/clients (gen/once {:f :read}))
   ;; Cut leader<->standby. The leader stays up + serving but goes Solo.
   (gen/nemesis (gen/once {:type :info :f :partition}))
   ;; Un-fsync'd writes onto the still-serving Solo leader: acked, but never shipped.
   (gen/clients (gen/time-limit 3 (repeat {:f :write :file "h"})))
   ;; The standby promotes + fences the old leader; the client re-routes to it.
   (gen/nemesis (gen/once {:type :info :f :await-serving}))
   ;; fsync of the file on the new leader: must FAIL (the Solo writes are gone).
   (gen/clients (gen/once {:f :fsync :file "h"}))
   (gen/sleep 1)
   ;; Clean write + fsync on the new leader: must succeed (the guard).
   (gen/clients (gen/once {:f :write :file "h"}))
   (gen/clients (gen/once {:f :fsync :file "h"}))
   (gen/sleep 2)
   ;; Heal (restores connectivity + canonical roles) so teardown + final read are clean.
   (gen/nemesis (gen/once {:type :info :f :heal-partition}))
   (gen/sleep 3)
   (gen/clients (gen/once {:f :read}))))

(defn honesty-transparent-gen
  "Transparency (per-fid): open a file, write un-fsync'd values onto a healthy
  Connected leader (shipped to the standby's tail), then a CLEAN kill-leader. The
  standby promotes + replays the tail so the writes SURVIVE; the client re-routes and
  a fsync of that file must SUCCEED, the writes really are durable, in contrast to the
  conservative ESTALE the cold-restart/partition cases return. The carried-forward
  (untainted) lineage token keeps the :ok honest. The checker still catches a kept
  token over a write that was actually lost."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "h"}))
   (gen/clients (gen/once {:f :read}))
   ;; Acknowledged, un-fsync'd writes on the Connected leader -> shipped to the standby.
   (gen/clients (gen/time-limit 6 (repeat {:f :write :file "h"})))
   (gen/sleep 1)
   ;; Clean kill-leader: the standby promotes + replays the tail, so the writes live on.
   (gen/nemesis (gen/once {:type :info :f :kill-leader}))
   (gen/nemesis (gen/once {:type :info :f :await-serving}))
   ;; fsync of the file on the promoted leader: must be :ok (transparent, survived).
   (gen/clients (gen/once {:f :fsync :file "h"}))
   (gen/sleep 1)
   ;; Clean write + fsync (the guard).
   (gen/clients (gen/once {:f :write :file "h"}))
   (gen/clients (gen/once {:f :fsync :file "h"}))
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn crossfid-gen
  "Cross-fid honesty: the per-fd POSIX case a single global barrier cannot catch. Open
  TWO files a and b, write un-fsync'd values to BOTH, then a cold double-restart loses
  both. fsync(a) fails; the app, per POSIX since the ESTALE on a says nothing about b,
  redoes ONLY a, and fsync(a) succeeds. fsync(b) MUST STILL FAIL: a's redo must not
  discharge b's lost write. The fsync obligation is per-fid, so b stays independent of
  a's redo. A final clean write+fsync keeps the last fsync :ok for the healthy-window
  guard."
  []
  (gen/phases
   (gen/clients (gen/once {:f :open :file "a"}))
   (gen/clients (gen/once {:f :open :file "b"}))
   (gen/clients (gen/once {:f :read}))
   ;; Un-fsync'd writes to BOTH files under the same lineage.
   (gen/clients (gen/once {:f :write :file "a"}))
   (gen/clients (gen/once {:f :write :file "b"}))
   (gen/sleep 1)
   ;; Cold double restart: both un-fsync'd writes are lost.
   (gen/nemesis (gen/once {:type :info :f :kill-both}))
   (gen/sleep 1)
   (gen/nemesis (gen/once {:type :info :f :heal-restart}))
   (gen/sleep 5)
   ;; fsync(a) fails -> the app redoes ONLY a (per-fd) -> fsync(a) succeeds.
   (gen/clients (gen/once {:f :fsync :file "a"}))
   (gen/clients (gen/once {:f :write :file "a"}))
   (gen/clients (gen/once {:f :fsync :file "a"}))
   ;; fsync(b) must STILL fail: a's redo did not discharge b.
   (gen/clients (gen/once {:f :fsync :file "b"}))
   (gen/sleep 1)
   ;; Guard: a clean write+fsync so the LAST fsync is :ok.
   (gen/clients (gen/once {:f :write :file "a"}))
   (gen/clients (gen/once {:f :fsync :file "a"}))
   (gen/sleep 2)
   (gen/clients (gen/once {:f :read}))))

(defn ha-test [opts]
  (let [durability? (or (:fsync-honesty opts) (:fsync-honesty-partition opts)
                        (:fsync-transparency opts) (:fsync-crossfid opts)
                        (:fsync-metadata opts) (:fsync-multihandle opts)
                        (:fsync-dirent opts) (:fsync-rename opts)
                        (:fsync-metadata-transparency opts) (:fsync-multifailover opts))]
   (merge tests/noop-test
         opts
         {:name      "zerofs-ha"
          :os        os/noop
          :db        (db)
          ;; Per-fid durability scenarios hold files open and fsync them by name; the
          ;; set workload uses the SetClient.
          :client    (if durability?
                       (->DurabilityClient (str (:work-dir opts) "/mnt/dc") (atom {}) (atom 0))
                       (->SetClient (str (:work-dir opts) "/mnt") (:fsync opts)
                                    (atom -1) (atom {}) (atom nil)))
          :nemesis   (ha-nemesis)
          :checker   (if durability?
                       (checker/compose
                        {:fsync-honesty (cond
                                          (or (:fsync-metadata opts) (:fsync-metadata-transparency opts))
                                          (metadata-honesty-checker)
                                          (or (:fsync-dirent opts) (:fsync-rename opts))
                                          (dirent-honesty-checker)
                                          :else (fsync-honesty-checker))
                         :perf          (checker/perf)})
                       (checker/compose
                        ;; :set  -> no :ok add lost, no :ok-removed value resurrected.
                        ;; :statfs -> the usage-stats inode count tracks the live set
                        ;;            (catches a regressed stats/allocator or a bad delete).
                        ;; :content -> no file's content disagrees with its name
                        ;;             (catches inode-id reuse the set check can't see).
                        {:set     (set-checker)
                         :statfs  (statfs-checker)
                         :content (content-checker)
                         :perf    (checker/perf)}))
          :generator (cond
                       (:fsync-dirent opts) (dirent-gen)
                       (:fsync-rename opts) (rename-gen)
                       (:fsync-metadata-transparency opts) (metadata-transparency-gen)
                       (:fsync-multifailover opts) (multifailover-gen)
                       (:fsync-multihandle opts) (multihandle-gen)
                       (:fsync-metadata opts) (metadata-gen)
                       (:fsync-crossfid opts) (crossfid-gen)
                       (:fsync-transparency opts) (honesty-transparent-gen)
                       (:fsync-honesty-partition opts) (honesty-partition-gen)
                       (:fsync-honesty opts) (honesty-gen)
                       (:fsync opts)
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
                       :else
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
          :pure-generators true})))

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
    :default true]
   [nil "--fsync-honesty" "Run the fsync-durability-honesty scenario: a burst of un-fsync'd writes, then kill-both + cold restart, then a fsync. Asserts a successful fsync never reports success for the lost writes."
    :default false]
   [nil "--fsync-honesty-partition" "fsync-honesty under a partition (the Solo-at-takeover case): un-fsync'd writes onto a partitioned Solo leader the standby never receives, then a fsync on the promoted leader must fail rather than report success. The lineage token regenerates on takeover (the Solo term marks it), so it fails honestly."
    :default false]
   [nil "--fsync-transparency" "Transparency (per-fid): un-fsync'd writes to an open file on a Connected leader, then a CLEAN kill-leader. The standby replays the tail so the writes survive, and a fsync of that file must SUCCEED (carried-forward untainted lineage token), in contrast to the conservative ESTALE of the cold/partition cases."
    :default false]
   [nil "--fsync-crossfid" "Cross-fid (per-fd POSIX): two open files written un-fsync'd, a cold double-restart loses both, fsync(a) fails, the app redoes ONLY a and fsync(a) succeeds, then fsync(b) MUST STILL fail. The fsync obligation is per-fid, so a's redo does not discharge b's lost write."
    :default false]
   [nil "--fsync-metadata" "Metadata honesty (FUSE fid-mapping): ftruncate(fd) un-fsync'd is a setattr the mount lands on the per-inode fid, not the open handle the app fsyncs. A cold double-restart loses the truncate; fsync(fd) MUST fail. A verified fsync aggregates every fid bound to the inode and returns ESTALE rather than reporting success over the lost truncate."
    :default false]
   [nil "--fsync-multihandle" "Multiple-open-handle honesty: open the same file twice, write un-fsync'd through one handle, cold double-restart loses it, fsync through the OTHER handle MUST fail (POSIX fsync(fd) persists the whole file). A verified fsync aggregates every open handle of the inode and returns ESTALE rather than reporting success over the sibling handle's lost write."
    :default false]
   [nil "--fsync-dirent" "Directory-entry honesty: mkdir un-fsync'd, cold double-restart loses the entry, fsyncdir of the parent MUST fail. fsync of a directory verifies the entries created or removed in it."
    :default false]
   [nil "--fsync-rename" "Cross-directory rename honesty: move a file from src to dst un-fsync'd, cold double-restart loses the move, fsyncdir of the SOURCE MUST fail. A rename is attributed to both directories, so fsync of either covers it."
    :default false]
   [nil "--fsync-metadata-transparency" "Metadata transparency: ftruncate un-fsync'd on a Connected leader, clean kill-leader. The standby replays the tail so the truncate survives, and a fsync must SUCCEED with the size intact. Verifies tail replay carries setattr."
    :default false]
   [nil "--fsync-multifailover" "Cross-term honesty: a write survives a transparent failover (token kept), then a cold restart regenerates the token, so a fsync over the carried-then-lost write MUST fail. Verifies a kept token is still invalidated by a later lineage break."
    :default false]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn ha-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
