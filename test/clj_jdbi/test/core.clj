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


(with-derby
  (let [dbi (create-dbi "jdbc:derby:testing")]
    (deftest basic-connectivity
      (is (not (nil? dbi))))
    (deftest insert-select
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
