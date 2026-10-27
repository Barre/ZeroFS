(ns zerofs-ha.ninep
  "Read-only 9P probes for HA readiness."
  (:require [jepsen.util :as util])
  (:import [java.io EOFException IOException]
           [java.net StandardProtocolFamily UnixDomainSocketAddress]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels SocketChannel]))

(defn- buffer [size]
  (doto (ByteBuffer/allocate size) (.order ByteOrder/LITTLE_ENDIAN)))

(defn- read-buffer! [^SocketChannel channel ^ByteBuffer buf]
  (while (.hasRemaining buf)
    (when (neg? (.read channel buf))
      (throw (EOFException. "9P probe connection closed"))))
  (.flip buf)
  buf)

(defn- exchange! [^SocketChannel channel type tag ^bytes payload response-type]
  (let [request (doto (buffer (+ 7 (alength payload)))
                  (.putInt (+ 7 (alength payload)))
                  (.put (unchecked-byte type))
                  (.putShort (short tag))
                  (.put payload)
                  (.flip))]
    (while (.hasRemaining request)
      (.write channel request)))
  (let [header (read-buffer! channel (buffer 7))
        size (.getInt header)
        type (Byte/toUnsignedInt (.get header))
        reply-tag (.getShort header)]
    (when-not (and (<= 7 size 4096) (= response-type type) (= tag reply-tag))
      (throw (IOException. "Invalid or unsuccessful 9P probe response")))
    (read-buffer! channel (buffer (- size 7)))))

(defn writer-epoch
  "Return the serving writer epoch, or nil for an unavailable/fenced server.
  Tgetlineage is lease-gated, unlike accepting a socket or negotiating Tversion.
  The timeout interrupts and closes the channel even if a paused server accepts."
  [socket-path]
  (util/timeout 1000 nil
    (try
      (with-open [channel (SocketChannel/open StandardProtocolFamily/UNIX)]
        (.connect channel (UnixDomainSocketAddress/of ^String socket-path))
        ;; Tversion/Rversion, followed by ZeroFS Tgetlineage/Rgetlineage.
        ;; These read-only requests carry no mutation envelope.
        (let [version (.getBytes "9P2000.L.Z" "UTF-8")
              payload (doto (buffer (+ 6 (alength version)))
                        (.putInt 4096)
                        (.putShort (short (alength version)))
                        (.put version))]
          (exchange! channel 100 0 (.array payload) 101))
        (let [lineage (exchange! channel 233 1 (byte-array 0) 234)]
          (when (= 16 (.remaining lineage))
            ;; The first u64 is the durability token, not the writer epoch.
            (let [epoch (.getLong lineage 8)]
              (when (pos? epoch) epoch)))))
      (catch IOException _ nil))))
