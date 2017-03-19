(ns nsq.core
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.java.io       :as io]
            [clojure.core.async :as async]
            [clojure.core.async :refer [<!! >!! close! chan buffer alt!! timeout]]
            [clj-http.client :as http]
            [slingshot.slingshot :refer [try+ throw+]]
            [cheshire.core            :as json]
            [jepsen.core           :as core]
            [jepsen.util           :refer [meh log-op]]
            [jepsen.codec          :as codec]
            [jepsen.core           :as core]
            [jepsen.control        :as c]
            [jepsen.control.util   :as cu]
            [jepsen.client         :as client]
            [jepsen.db             :as db]
            [jepsen.generator      :as gen]
            [jepsen.os.debian         :as debian]
            [knossos.core          :as knossos]
            [knossos.op            :as op])
  (:import (com.youzan.nsq.client.entity NSQConfig))
  (:import (com.youzan.nsq.client.entity NSQMessage))
  (:import (com.youzan.nsq.client ConsumerImplV2))
  (:import (com.youzan.nsq.client ProducerImplV2))
  (:import (com.youzan.nsq.client MessageHandler))
  )

(def nsqd-binary "/usr/bin/nsqd")
(def nsqd-config "/var/lib/nsqd.conf")
(def nsqd-logfile "/var/log/nsqd.log")
(def nsqd-pidfile "/var/run/nsqd.pid")
(def nsqd-data "/var/lib/nsqd")

(def nsqlookupd-binary "/usr/bin/nsqlookupd")
(def nsqlookupd-config "/var/lib/nsqlookupd.conf")
(def nsqlookupd-logfile "/var/log/nsqlookupd.log")
(def nsqlookupd-pidfile "/var/run/nsqlookupd.pid")
(def etcd-cluster-url "http://etcd0.example.com:2379/v2/keys/NSQMetaData/test-jepsen-dev-1")

(defn prepare-binary! 
  [test node]
  (info node "preparing the binary")
  (http/delete 
    (str etcd-cluster-url "?recursive=true") 
    {:throw-exceptions false :query-params {:recursive true}})
  ; You'll need debian testing for this
  (debian/install [:git-core])

   (c/su
    (c/cd "/opt/nsq"
          (when-not (cu/exists? "build/nsqd")
            (c/exec :mkdir :-p "/opt/nsq/build")
            (c/exec :chmod :-R "a+rwx" "/opt/nsq")
            (c/exec :rm :-f (c/lit "/opt/nsq/nsq-0.3.7-HA.1.5.3.1.linux-amd64.go1.7.4.tar.gz*"))
            (info node "downloading nsq")
            ;(c/exec :wget "https://github.com/absolute8511/nsq/releases/download/v0.3.7-HA.1.5.3.1/nsq-0.3.7-HA.1.5.3.1.linux-amd64.go1.7.4.tar.gz")
            (c/upload (.getFile (io/resource "nsq-0.3.7-HA.1.5.3.1.linux-amd64.go1.7.4.tar.gz")) "/opt/nsq/nsq-0.3.7-HA.1.5.3.1.linux-amd64.go1.7.4.tar.gz")
            (info node "tar nsq")
            (c/exec :tar :-xvzf "nsq-0.3.7-HA.1.5.3.1.linux-amd64.go1.7.4.tar.gz")
            (info node "copying nsq binary")
            (c/exec :cp :-rp (c/lit "nsq-0.3.7-HA.1.5.3.1.linux-amd64.go1.7.4/bin/*") "build/"))
          (info node "copying nsq")
          (c/exec :cp :-rp (c/lit "build/*") "/usr/bin/"))))

(defn start-nsqd!
  [test node]
  (info node "starting nsqd")
  (c/exec :echo
          (-> "nsqd.conf"
            io/resource 
            slurp)
          :> nsqd-config)
  (c/exec :start-stop-daemon :--start
          :--background
          :--make-pidfile
          :--pidfile nsqd-pidfile
          :--chdir "/opt/nsq"
          :--exec nsqd-binary
          :--no-close
          :--
          :-config nsqd-config
          :-data-path nsqd-data
          :>> nsqd-logfile
          (c/lit "2>&1")))

(defn start-nsqlookupd!
  [test node]
  (info node "starting nsqlookupd")
  (c/exec :echo
          (-> "nsqlookupd.conf"
            io/resource
            slurp)
          :> nsqlookupd-config)
  (c/exec :start-stop-daemon :--start
          :--background
          :--make-pidfile
          :--pidfile nsqlookupd-pidfile
          :--chdir "/opt/nsq"
          :--exec nsqlookupd-binary
          :--no-close
          :--
          :-config nsqlookupd-config
          :>> nsqlookupd-logfile
          (c/lit "2>&1"))
  (Thread/sleep 5000))

(def queue "jepsen.queue")

(defn init-queue
  [node]
  (info node "init queue topic")
  (try+
    (let [resp (http/get (str "http://" (name node) ":4161/listlookup") {:ignore-unknown-host? true})]
      (when-not (= 200 (:status resp))
        (info (str "listlookup failed " resp)))
      (let [leaderip (-> resp
                       :body
                       (json/parse-string #(keyword (.toLowerCase %)))
                       :data
                       :lookupdleader
                       :nodeip)]
        (http/post 
          (str "http://" leaderip ":4161/topic/create") 
          {:query-params {:topic queue :partition_num 1 :replicator 3 :syncdisk 1}})))
    (catch [:status 403] {:keys [request-time headers body]}
      (info "403" request-time headers))
    (catch [:status 404] {:keys [request-time headers body]}
      (info "NOT Found 404" request-time headers body))
    (catch [:status 400] {:keys [request-time headers body]}
      (info "400 " request-time headers body))
    (catch Object _
      (warn (:throwable &throw-context) "unexpected error")))
  (try+
    (http/post
      (str "http://" (name node) ":4151/channel/create")
      {:query-params {:topic queue :channel "jepsen-ch"}})
    (catch Object _
      (info "init channel failed"))))

(def db
  (reify db/DB
    (setup! [this test node]
      (prepare-binary! test node)
      (start-nsqlookupd! test node)
      (Thread/sleep 1000)
      (info node "nsqlookupd ready")
      (start-nsqd! test node)
      (Thread/sleep 1000)
      (info node "nsqd ready")
      (core/synchronize test)
      (Thread/sleep 5000)
      (init-queue node))

    (teardown! [_ test node]
      (c/su
        (meh (c/exec :killall :nsqd))
        (Thread/sleep 3000)
        (meh (c/exec :killall :-9 :nsqd))
        (c/exec :rm :-rf nsqd-pidfile nsqd-data))
      (info node "nsqd killed")
      (c/su
        (meh (c/exec :killall :nsqlookupd))
        (Thread/sleep 3000)
        (meh (c/exec :killall :-9 :nsqlookupd))
        (c/exec :rm :-rf nsqlookupd-pidfile))
      (info node "nsqlookupd killed"))))


(defn new-producer
  [conf]
  (let [client (ProducerImplV2. conf)]
    (.start client)
    client))

(defn new-consume-handler
  [rc]
  (reify MessageHandler
    (process [this msg]
      (let [value (codec/decode (.getMessageBody msg))]
        (info "===== got message:" value)
        (if (nil? value)
          (info "got nil message!!!"))
        (>!! rc value)
        (info "queued to chan: " value)))))

(defn new-consumer
  [conf rc]
  (let [client (ConsumerImplV2. conf (new-consume-handler rc))]
    (info "init consumer")
    (.setAutoFinish client (boolean 1))
    (.subscribe client (into-array String [queue]))
    (.start client)
    client))

(def ^NSQConfig nsqconf
  (let [conf (NSQConfig.)]
    (.setConsumerName conf "jepsen-ch")
    conf))

(defn dequeue!
  "Given a channel and an operation, dequeues a value and returns the
  corresponding operation."
  [to_ms rc op]
  (let [timeout-ch (async/timeout to_ms)]
    (alt!!
      timeout-ch (assoc op :type :fail :value :timeout)
      rc ([value] (assoc op :type :ok :value value)))))

(defrecord QueueClient [client consumer receive-ch]
  client/Client
  (setup! [this test node]
    (info "setup client for node" node)
    (.setLookupAddresses nsqconf (str (name node) ":4161"))
    (let [producer (new-producer nsqconf)
          rc (chan (buffer 10))
          cs (new-consumer nsqconf rc)]
      (Thread/sleep 30000)
      (assoc this :client producer :consumer cs :receive-ch rc)))

  (teardown! [this test]
    ; Close
    (meh (.close client))
    (meh (.close consumer))
    (info "client destroying ")
    (close! receive-ch))

  (invoke! [this test op]
    (case (:f op)
      :enqueue (do
                 (try 
                   (.publish client (codec/encode (:value op)) queue)
                   (assoc op :type :ok)
                   (catch Exception e
                     (warn e "pub failed")
                     (assoc op :type :fail))))

      :dequeue (dequeue! 5000 receive-ch op)

      :drain   (do
                 ; Note that this does more dequeues than strictly necessary
                 ; owing to lazy sequence chunking.
                 (->> (repeat op)                  ; Explode drain into
                   (map #(assoc % :f :dequeue)) ; infinite dequeues, then
                   (map (partial dequeue! 40000 receive-ch))  ; dequeue something
                   (take-while op/ok?)  ; as long as stuff arrives,
                   (interleave (repeat op))     ; interleave with invokes
                   (drop 1)                     ; except the initial one
                   (map (fn [completion]
                          (log-op completion)
                          (core/conj-op! test completion)))
                   dorun)
                 (assoc op :type :ok :value :exhausted)))))

(defn queue-client [] (QueueClient. nil nil nil))

