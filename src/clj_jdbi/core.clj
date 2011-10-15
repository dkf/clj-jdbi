(ns clj-jdbi.core
  (:import
   [org.skife.jdbi.v2 DBI Handle Query Folder2 PreparedBatch]
   [org.skife.jdbi.v2.tweak ConnectionFactory]
   [clojure.lang PersistentHashMap]
   [java.util Map]
   [javax.sql DataSource]))

(def ^DBI *dbi* nil)
(def ^Handle *handle* nil)

(defn create-dbi
  ([^String url] (DBI. url))
  ([^String url ^String user ^String pass] (DBI. url user pass)))

(defn create-dbi-from-data-source [^DataSource data-source]
  (DBI. data-source))

(defn create-dbi-from-connection-factory [^ConnectionFactory conn-factory]
  (DBI. conn-factory))

(defmacro bind-dbi [dbi & body]
  `(binding [*dbi* ~dbi]
    ~@body))

(defn dbi! [dbi]
  (alter-var-root (var *dbi*) (fn [_] dbi)))

(defmacro with-dbi ([dbi & body]
  `(bind-dbi ~dbi (do ~@body))))

(defn customize-dbi [^DBI dbi & options]
  (let [opts (merge {} (apply hash-map options))]
    (when-let [stmt-locator (:statement-locator opts)]
      (.setStatementLocator dbi stmt-locator))
    (when-let [stmt-rewriter (:statement-rewriter opts)]
      (.setStatementRewriter dbi stmt-rewriter))
    (when-let [tx-handler (:transaction-handler opts)]
      (.setTransactionHandler dbi tx-handler))
    (when-let [stmt-builder-factory (:statement-builder-factory opts)]
      (.setStatementBuilderFactory dbi stmt-builder-factory))
    (when-let [sql-log (:sql-log opts)]
      (.setSQLLog dbi sql-log))
    (when-let [timing-collector (:timing-collector opts)]
      (.setTimingCollector dbi timing-collector))
    dbi))

(defmacro with-handle [& body]
  `(binding [*handle* (. *dbi* open)]
    (try
      (do
	~@body)
      (finally (when-not (nil? *handle*)
		 (. *handle* close))))))

(defn handle! []
  (alter-var-root
   (var *handle*)
   (fn [old]
     ;(when (not (nil? old))
       ;(. old rollback))
     (. *dbi* open))))

(defn- keywordize-map
  "stolen from github.com/getwoven/plumbing"
  [m]
  (cond
   (instance? clojure.lang.IPersistentMap m)
     (into {}
      (map 
       (fn [[k v]]
	 [(if (string? k) (keyword k) k) (keywordize-map v)])
       m))
   (instance? clojure.lang.IPersistentList m)
     (map keywordize-map m)
   (instance? clojure.lang.IPersistentVector m)
     (into [] (map keywordize-map m))
     :else m))

;; (defmacro in-tx [& body]
;;   `(let [success# (AtomicBoolean. false)]
;;      (try
;;        (. *handle* begin)
;;        (let [r# ~@body]
;; 	 (. success# set false)
;; 	 r#)
;;        (finally
;; 	(comment (if (. success# get)
;; 	  (. *handle* commit)
;; 	  (. *handle* rollback)))))))

;; (defmacro with-tx [& body]
;;   `(if (nil? *handle*)
;;      (with-handle
;;        (in-tx
;; 	~@body))
;;      (in-tx ~@body)))



(defn select [sql & args]
  (doall
   (map
    (fn [^Map m] (keywordize-map (PersistentHashMap/create m)))
    (. *handle* select sql (into-array Object args)))))

(defn insert [sql & args]
  (. *handle* insert sql (into-array Object args)))

(defn update [sql & args]
  (. *handle* update sql (into-array Object args)))

(defn execute [sql & args]
  (. *handle* execute sql (into-array Object args)))

(defn query-create [sql & options]
  (let [opts (merge {:bind {}} (apply hash-map options))
	query (. *handle* createQuery sql)]
    (when-let [fetch-size (:fetch-size opts)]
      (.setFetchSize query fetch-size))
    (when-let [max-rows (:max-rows opts)]
      (.setMaxRows query max-rows))
    (when-let [max-field-size (:max-field-size opts)]
      (.setMaxFieldSize query max-field-size))
    (if (= true (:reverse opts))
      (.fetchReverse query))
    (if (= true (:forward opts))
      (.fetchForward query))
    (doseq [kv (:bind opts)]
      (.bind query (name (first kv)) (second kv)))
    query))

(defn query-list [^Query query]
  (doall
   (map
    (fn [^Map m] (keywordize-map (PersistentHashMap/create m)))
    (.list query))))


(defn query-first [^Query query]
  (let [^Map res (.first query)]
    (keywordize-map (PersistentHashMap/create res))))

(defn query-fold [^Query query initial folder]
  (.fold query initial
	 (reify Folder2
	   (fold [this acc rs ctx]
	     (folder acc rs ctx)))))

(defn batch-prepare [sql]
  (. *handle* prepareBatch sql))

(defn batch-add-map [^PreparedBatch batch m]
  (let [^Map stringified (into {} (map #(vector (name (first %)) (second %)) m))]
    (.add batch stringified)))

(defn batch-add-positional [^PreparedBatch batch & positional]
  (let [^"[Ljava.lang.Object;" a (into-array Object positional)]
    (.add batch a)))

(defn batch-execute [^PreparedBatch batch]
  (.execute batch))
