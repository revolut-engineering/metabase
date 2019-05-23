(ns metabase.driver.exasol
  (:require [clojure.java.jdbc :as jdbc]
            [clojure
             [set :as set :refer [rename-keys]]
             [string :as s]]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db.spec :as dbspec]
            [metabase.driver.generic-sql :as sql]
            [metabase.util
             [honeysql-extensions :as hx]
             [ssh :as ssh]]))

(defrecord ExasolDriver []
  clojure.lang.Named
  (getName [_] "Exasol"))

(def ^:private ^:const column->base-type
  "Map of Exasol column types -> Field base types. Add more mappings here as you come across them."
  {:BIGINT         :type/BigInteger
   :BOOLEAN        :type/Boolean
   :CHAR           :type/Text
   :DATE           :type/Date
   :DECIMAL        :type/Decimal
   :FLOAT          :type/Float
   :GEOMETRY       :type/Text
   :INTEGER        :type/Integer
   :SMALLINT       :type/Integer
   :TIMESTAMP      :type/DateTime
   :TINYINT        :type/Integer
   :VARCHAR        :type/Text
   (keyword "DOUBLE PRECISION")               :type/Float
   (keyword "INTERVAL DAY TO SECOND")         :type/*
   (keyword "INTERVAL YEAR TO MONTH")         :type/*
   (keyword "LONG VARCHAR")                   :type/Text
   (keyword "TIMESTAMP WITH LOCAL TIME ZONE") :type/DateTime})


(defn- connection-details->spec [{:keys [host port schema querytimeout], :as opts}]
    ; :or   {host "localhost", port 8563, db ""}
    ; :as   details}]
  (-> (merge {:classname   "com.exasol.jdbc.EXADriver"
              :subprotocol "exa"
              :subname     (str host ":" port)
              ; :querytimeout querytimeout
              ; :autocommit 0
              :encryption 0
              :schema schema}
             (dissoc opts :host :port :dbname :db :ssl))
      (sql/handle-additional-options opts)))

(defn- unix-timestamp->timestamp [expr seconds-or-milliseconds]
  (hsql/call :from_posix_time (case seconds-or-milliseconds
                                :seconds      expr
                                :milliseconds (hx// expr 1000))))


(defn- date-trunc [unit expr] (hsql/call :date_trunc (hx/literal unit) (hx/->timestamp expr)))
(defn- extract    [unit expr] (hsql/call :extract    unit              (hx/->timestamp expr)))

(def ^:private extract-integer (comp hx/->integer extract))

(def ^:private ^:const one-day (hsql/raw "INTERVAL '1 day'"))

(defn- date [unit expr]
  (case unit
    :default         expr
    :minute          (date-trunc :minute expr)
    :minute-of-hour  (extract-integer :minute expr)
    :hour            (date-trunc :hour expr)
    :hour-of-day     (extract-integer :hour expr)
    :day             (hx/->date expr)
    ;; Postgres DOW is 0 (Sun) - 6 (Sat); increment this to be consistent with Java, H2, MySQL, and Mongo (1-7)
    :day-of-week     (hx/inc (extract-integer :dow expr))
    :day-of-month    (extract-integer :day expr)
    :day-of-year     (extract-integer :doy expr)
    ;; Postgres weeks start on Monday, so shift this date into the proper bucket and then decrement the resulting day
    :week            (hx/- (date-trunc :week (hx/+ (hx/->timestamp expr) one-day))
                           one-day)
    :week-of-year    (extract-integer :week (hx/+ (hx/->timestamp expr) one-day))
    :month           (date-trunc :month expr)
    :month-of-year   (extract-integer :month expr)
    :quarter         (date-trunc :quarter expr)
    :quarter-of-year (extract-integer :quarter expr)
    :year            (extract-integer :year expr)))

(defn- date-interval [unit amount]
  (hsql/raw (format "(NOW() + INTERVAL '%d %s')" (int amount) (name unit))))

(defn- string-length-fn [field-key]
  (hsql/call :char_length (hx/cast :Varchar field-key)))


(def ^:private exasol-date-formatters (driver/create-db-time-formatters "yyyy-MM-dd HH:mm:ss"))
(def ^:private exasol-db-time-query "select to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')")

(u/strict-extend ExasolDriver
                 driver/IDriver
                 (merge (sql/IDriverSQLDefaultsMixin)
                        {:date-interval     (u/drop-first-arg date-interval)
                         :details-fields    (constantly (ssh/with-tunnel-config
                                                          [{:name         "host"
                                                            :display-name "Host"
                                                            :default      "localhost"}
                                                           {:name         "port"
                                                            :display-name "Port"
                                                            :type         :integer
                                                            :default      8563}
                                                           {:name         "schema"
                                                            :display-name "Schema"
                                                            :placeholder  "sys"
                                                            :required     true}
                                                           {:name         "user"
                                                            :display-name "Database username"
                                                            :placeholder  "sys"
                                                            :required     true}
                                                           {:name         "password"
                                                            :display-name "Database password"
                                                            :type         :password
                                                            :placeholder  "exasol"}]))
                         :current-db-time   (driver/make-current-db-time-fn exasol-db-time-query exasol-date-formatters)
;          :features                          (constantly (set/union #{:set-timezone
;                                                                      :basic-aggregations
;                                                                      :standard-deviation-aggregations
;                                                                      :expressions
;                                                                      :native-parameters
;                                                                      :nested-queries
;                                                                      :expression-aggregations
;                                                                      :binning
;                                                                      :native-query-params}))
                         })
                 sql/ISQLDriver
                 (merge (sql/ISQLDriverDefaultsMixin)
                        {:column->base-type         (u/drop-first-arg column->base-type)
                         :connection-details->spec  (u/drop-first-arg connection-details->spec)
                         :date                      (u/drop-first-arg date)
                         :set-timezone-sql          (constantly "ALTER SESSION SET time_zone = %s;")
                         :string-length-fn          (u/drop-first-arg string-length-fn)
                         :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(defn -init-driver
  "Register the Exasol driver when found on the classpath"
  []
  ;; only register the Exasol driver if the JDBC driver is available
  (driver/register-driver! :exasol (ExasolDriver.)))
;; only register the Exasol driver if the JDBC driver is available
;; (when (u/ignore-exceptions
;;  (Class/forName "com.exasol.jdbc.EXADriver"))
;; (driver/register-driver! :exasol (ExasolDriver.))))
