# `clj-jdbi`

very basic, very alpha clojure wrapper for jdbi (http://jdbi.org/).
not nearly ready for real use yet.

## Usage

```clojure
user> (use 'clj-jdbi.core)
nil
user> (def local-db (create-dbi "jdbc:mysql://localhost/test" "username" "password"))
#'user/local-db
user> (with-dbi local-db
	(with-handle
	  (select "select * from foo limit 2")))
({:name "foo", :id 1} {:name "bar", :id 2})
```

## Todo
* `JDBI` has a ton of features, `clj-jdbi` exposes almost none of them
