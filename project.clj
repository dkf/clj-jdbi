(defproject clj-jdbi "1.0.0-SNAPSHOT"
  :description "clojure wrapper around jdbi"
  :warn-on-reflection true
  :dependencies [[org.jdbi/jdbi "2.27"]]
  :dev-dependencies [[com.h2database/h2 "1.3.158"]
		     [org.apache.derby/derby "10.2.2.0"]
		     [lein-clojars "0.6.0"]])
