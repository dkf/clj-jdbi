(ns clj-jdbi.core
  (:import
   [org.skife.jdbi.v2 DBI]
   [clojure.lang PersistentHashMap]))

(def *dbi* nil)
(def *handle* nil)

(defn create-dbi
  ([url] (DBI. url))
  ([url user pass] (DBI. url user pass)))

(defmacro bind-dbi [dbi & body]
  `(binding [*dbi* ~dbi]
    ~@body))

(defn dbi! [dbi]
  (alter-var-root (var *dbi*) (fn [_] dbi)))

(defmacro with-dbi ([dbi & body]
  `(bind-dbi ~dbi (do ~@body))))

(defmacro with-handle [& body]
  `(binding [*handle* (. *dbi* open)]
    (try
      (do
	~@body)
      (finally (when (not (nil? *handle*))
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
    #(keywordize-map (PersistentHashMap/create %))
    (. *handle* select sql (into-array Object args)))))

(defn insert [sql & args]
  (. *handle* insert sql (into-array Object args)))

(defn update [sql & args]
  (. *handle* update sql (into-array Object args)))

(defn execute [sql & args]
  (. *handle* execute sql (into-array Object args)))