(ns metabase.integrations.slack
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.core.memoize :as memoize]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [medley.core :as m]
            [metabase.models.setting :as setting :refer [defsetting]]
            [metabase.util :as u]
            [metabase.util
             [i18n :refer [deferred-tru trs tru]]
             [schema :as su]]
            [schema.core :as s]))

;; Define a setting which captures our Slack api token
(defsetting slack-token (deferred-tru "Slack API bearer token obtained from https://api.slack.com/web#authentication"))

(def ^:private ^String slack-api-base-url "https://slack.com/api")
(def ^:private ^String files-channel-name "metabase_files")

(defn slack-configured?
  "Is Slack integration configured?"
  []
  (boolean (seq (slack-token))))

(defn- handle-error [body]
  (let [invalid-token? (= (:error body) "invalid_auth")
        message        (if invalid-token?
                         (tru "Invalid token")
                         (tru "Slack API error: {0}" (:error body)))
        error          (if invalid-token?
                         {:error-code (:error body)
                          :errors     {:slack-token message}}
                         {:error-code (:error body)
                          :message    message
                          :response   body})]
    (log/warn (u/pprint-to-str 'red error))
    (throw (ex-info message error))))

(defn- handle-response [{:keys [status body]}]
  (with-open [reader (io/reader body)]
    (let [body (json/parse-stream reader true)]
      (if (and (= 200 status) (:ok body))
        body
        (handle-error body)))))

(defn- do-slack-request [request-fn endpoint request]
  (let [token (or (get-in request [:query-params :token])
                  (get-in request [:form-params :token])
                  (slack-token))]
    (when token
      (let [url     (str slack-api-base-url "/" (name endpoint))
            _       (log/trace "Slack API request: %s %s" (pr-str url) (pr-str request))
            request (merge-with merge
                      {:query-params   {:token token}
                       :as             :stream
                       ;; use a relatively long connection timeout (10 seconds) in cases where we're fetching big
                       ;; amounts of data -- see #11735
                       :conn-timeout   10000
                       :socket-timeout 10000}
                      request)]
        (try
          (handle-response (request-fn url request))
          (catch Throwable e
            (throw (ex-info (.getMessage e) (merge (ex-data e) {:url url, :request request}) e))))))))

(defn GET
  "Make a GET request to the Slack API."
  [endpoint & {:as query-params}]
  (do-slack-request http/get endpoint {:query-params query-params}))

(defn POST
  "Make a POST request to the Slack API."
  (partial do-slack-request http/post :form-params))

(def ^{:arglists '([& {:as args}])} channels-list
  "Calls Slack api `channels.list` function and returns the list of available channels."
  (comp :channels (partial GET :channels.list, :exclude_archived true, :exclude_members true)))

(def ^{:arglists '([& {:as args}])} channels-private-list
  "Calls Slack api `channels.list` function and returns the list of available channels."
  (comp :channels (partial GET :conversations.list, :exclude_archived true, :types "private_channel")))

(def ^{:arglists '([& {:as args}])} users-list
  "Calls Slack api `users.list` function and returns the list of available users."
  (comp :members (partial GET :users.list)))

(def ^:private ^String channel-missing-msg
  (str "Slack channel named `metabase_files` is missing! Please create the channel in order to complete "
       "the Slack integration. The channel is used for storing graphs that are included in pulses and "
       "MetaBot answers."))

(defn- maybe-get-files-channel
  "Return the `metabase_files channel (as a map) if it exists."
  []
  (some (fn [channel] (when (= (:name channel) files-channel-name)
                        channel))
        (channels-list :exclude_archived false)))

(defn- files-channel* []
  (or (channel-with-name files-channel-name)
      (let [message (str (tru "Slack channel named `metabase_files` is missing!")
                         " "
                         (tru "Please create or unarchive the channel in order to complete the Slack integration.")
                         " "
                         (tru "The channel is used for storing graphs that are included in Pulses and MetaBot answers."))]
        (log/error (u/format-color 'red message))
        (throw (ex-info message {:status-code 400})))))

(def ^{:arglists '([])} files-channel
  "Calls Slack api `channels.info` to check whether a channel named #metabase_files exists. If it doesn't, throws an
  error that advices an admin to create it."
  ;; If the channel has successfully been created we can cache the information about it from the API response. We need
  ;; this information every time we send out a pulse, but making a call to the `coversations.list` endpoint everytime we
  ;; send a Pulse can result in us seeing 429 (rate limiting) status codes -- see
  ;; https://github.com/metabase/metabase/issues/8967
  ;;
  ;; Of course, if `files-channel*` *fails* (because the channel is not created), this won't get cached; this is what
  ;; we want -- to remind people to create it
  (memoize/ttl files-channel* :ttl/threshold (u/hours->ms 6)))

(def ^:private NonEmptyByteArray
  (s/constrained
   (Class/forName "[B")
   not-empty
   "Non-empty byte array"))

(s/defn upload-file!
  "Calls Slack API `files.upload` endpoint and returns the URL of the uploaded file."
  [file :- NonEmptyByteArray, filename :- su/NonBlankString, channel-id :- su/NonBlankString]
  {:pre [(seq (slack-token))]}
  (let [response (http/post (str slack-api-base-url "/files.upload") {:multipart [{:name "token",    :content (slack-token)}
                                                                                  {:name "file",     :content file}
                                                                                  {:name "filename", :content filename}
                                                                                  {:name "channels", :content channel-id}]
                                                                      :as        :json})]
    (if (= 200 (:status response))
      (u/prog1 (get-in response [:body :file :url_private])
        (log/debug (trs "Uploaded image") <>))
      (log/warn (trs "Error uploading file to Slack:") (u/pprint-to-str response)))))

(s/defn post-chat-message!
  "Calls Slack API `chat.postMessage` endpoint and posts a message to a channel. `attachments` should be serialized
  JSON."
  [channel-id :- su/NonBlankString, text-or-nil :- (s/maybe s/Str) & [attachments]]
  ;; TODO: it would be nice to have an emoji or icon image to use here
  (POST "chat.postMessage"
        {:channel     channel-id
         :username    "MetaBot"
         :icon_url    "http://static.metabase.com/metabot_slack_avatar_whitebg.png"
         :text        text-or-nil
         :attachments (when (seq attachments)
                        (json/generate-string attachments))}))

(def ^{:arglists '([& {:as params}])} websocket-url
  "Return a new WebSocket URL for [Slack's Real Time Messaging API](https://api.slack.com/rtm)
   This makes an API request so don't call it more often than needed."
  (comp :url (partial GET :rtm.start)))
