(ns clj-jdbi.test.core
  (:use [clj-jdbi.core] :reload)
  (:use [clojure.test])
  (:import [org.apache.derby.jdbc EmbeddedDataSource]
	   [java.io File]))

(def data-source nil)

(defmacro ignore-exception [& body]
  `(try
     ~@body
     (catch Throwable _# nil)))

(defmacro with-derby [& body]
  `(try
     (let [f# (File. "build/db")
	   ignore1# (System/setProperty "derby.system.home", "build/db")
	   ignore2# (.mkdirs f#)
	   ds# (EmbeddedDataSource.)
	   ignore3# (.setCreateDatabase ds# "create")
	   ignore4# (.setDatabaseName ds# "testing")
	   conn# (.getConnection ds#)
	   ]
       (.close conn#)
       (binding [data-source ds#]
	 (with-dbi (create-dbi "jdbc:derby:testing")
	   (with-handle
	     (try
	       (ignore-exception (execute "delete from foo"))
	       (ignore-exception (execute "drop table foo"))
	       (execute "create table foo (id integer, name varchar(50), value integer)"))))
	 ~@body))
     (finally
      nil
      )))


(defn- create-test-dbi []
  (create-dbi "jdbc:derby:testing"))

(defmacro insert-test-data []
  `(doseq [x# (range 1000)]
    (insert "insert into foo values (:id, :name, :val)" x# (str "name" x#) (* 10 x#))))


(deftest insert-select
  (let [dbi (create-test-dbi)]
    (with-derby
      (with-dbi dbi
	(with-handle
	  (is (=
	       (:c (first (select "select count(*) c from foo"))))
	      0)
	  (insert "insert into foo values (1, 'foobar', 12)")
	  (is (=
	       (:name (first (select "select * from foo")))
	       "foobar"))
	  (is (=
	       (hash-map :id 1 :name "foobar", :value 12)
	       (first (select "select * from foo"))))
	  (update "update foo set name = ? where id = 1" "baz")
	  (is (=
	       (:name (first (select "select * from foo")))
	       "baz"))
	  )))
    ))

(deftest query-max-rows
  (with-derby
    (with-dbi (create-test-dbi)
      (with-handle
	(insert-test-data)
	(let [query (query-create "select * from foo" :max-rows 10)
	      result (query-list query)]
	  (is (= (count result) 10))))
      )))

(deftest query-bind
  (with-derby
    (with-dbi (create-test-dbi)
      (with-handle
	(insert-test-data)
	(let [query (query-create "select * from foo where id < :id" :bind {:id 5})
	      result (query-list query)]
	  (is (= (count result) 5))
	  (is (= (set (map :id result))
		 (set (range 5))))
	   )))))

(deftest test-query-first
  (with-derby
    (with-dbi (create-test-dbi)
      (with-handle
	(insert-test-data)
	(let [query (query-create "select count(*) c from foo")
	      result (query-first query)]
	  (is (map? result))
	  (is (= (:c result)
		 1000))
	   )))))

(deftest fold
  (with-derby
    (with-dbi (create-test-dbi)
      (with-handle
	(insert-test-data)
	(let [query (query-create "select id from foo" :fetch-size 10)
	      result (query-fold query 0 (fn [acc rs ctx] (+ acc (. rs getInt "id"))))]
	  (is (= (apply + (range 1000))
		 result))
	   )))))

(deftest batch
  (with-derby
    (with-dbi (create-test-dbi)
      (with-handle
	(let [batch (batch-prepare "insert into foo values (:id, :name, :val)")]
	  (batch-add-positional batch 1 "foo" 10)
	  (batch-add-map batch {:id 2 :name "bar" :val 20})
	  (let [batch-result (batch-execute batch)]
	    (is (= (count batch-result) 2))
	    (is (java.util.Arrays/equals batch-result (int-array [1 1]))))
	  (let [contents (select "select * from foo order by id")]
	    (is (= (count contents) 2))
	    (is (= contents
		   [{:id 1 :name "foo" :value 10} {:id 2 :name "bar" :value 20}])))
	   )))))
