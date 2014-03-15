(ns taurus.core
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [>! >!! <! <!! chan go go-loop sliding-buffer]]
            [clojure.algo.monads :as monads :refer [domonad state-m]]
            [clojure.data.priority-map :refer [priority-map-keyfn-by]]
             )
  (:import [quickfix 
            Application SessionID Message SessionSettings FileStoreFactory FileLogFactory DefaultMessageFactory
            SocketAcceptor SocketInitiator Session]
           [quickfix.fix44 OrderCancelRequest NewOrderSingle ExecutionReport]
           [quickfix.field SenderCompID TargetCompID OrigClOrdID ClOrdID Symbol Side TransactTime OrdType OrderQty Price OrderID ExecID
            ExecType OrdStatus LeavesQty CumQty AvgPx]
           [java.io FileInputStream])
  (:gen-class :main true)
  )

(defn new-guid [] (str (java.util.UUID/randomUUID))) 

(def exchange-chan (chan))
(def execution-chan (chan (sliding-buffer 1024)))

(go-loop [[order side price remaining-quantity] (<! execution-chan)]
         (let [{:keys [symbol id quantity sender target]} order
               s (.charAt (.toString side) 0)
               [exec-status order-status] (cond
                                            (= quantity remaining-quantity) [ExecType/NEW OrdStatus/NEW]
                                            (= 0 remaining-quantity) [ExecType/FILL OrdStatus/FILLED]
                                            :else [ExecType/PARTIAL_FILL OrdStatus/PARTIALLY_FILLED])]
           
           (let [exec-id (new-guid)
                 e (ExecutionReport. (OrderID. id) (ExecID. exec-id) (ExecType. exec-status) (OrdStatus. order-status)
                                     (Side. s) (LeavesQty. remaining-quantity) 
                                     (CumQty. (- quantity remaining-quantity)) (AvgPx. price))]
             (doto e 
             (.set (Symbol. symbol))
             )
             (Session/sendToTarget e target sender)))
         (recur (<! execution-chan))) 

(declare handle-new-order)

(defn create-chan [symbol]
  (fn [exchange-map]
    (let [c (exchange-map symbol)]
      (if c
        [c exchange-map]
        (let [new-chan (chan)]
          (do
            (go-loop [book {1 (priority-map-keyfn-by first >) 2 (priority-map-keyfn-by first <)}
                      order (<! new-chan)] 
                     (let [m (handle-new-order order)
                           [_ new-book] (m book)]
                       (recur new-book (<! new-chan))))
            [new-chan (assoc exchange-map symbol new-chan)]))
        ))))

(go-loop [symbols {}
          order (<! exchange-chan)]
         (let [m (create-chan (order :symbol)) 
               [c new-symbols] (m symbols)]
           (>! c order)
           (recur new-symbols (<! exchange-chan)))  
)         

(defmacro get-fix-fields [msg vars exprs]
  (let [b (mapcat (fn [v]
                    `[~v (new ~v)]
                    ) vars)
        g (map (fn [v] `(.getField ~v)) vars)
        ]
    `(let [~@b] (doto ~msg ~@g) ~exprs)
    ))

(defn new-order [msg]
  (let [header (.getHeader msg)
        session (get-fix-fields header [SenderCompID TargetCompID] {:sender (.getValue SenderCompID) :target (.getValue TargetCompID)}) 
        order (get-fix-fields msg [Symbol Price OrderQty Side ClOrdID]
                              (into session {:id (.getValue ClOrdID) 
                                            :symbol (.getValue Symbol) 
                                            :price (.getValue Price) 
                                            :quantity (.getValue OrderQty) 
                                            :side (-> (.getValue Side) (.toString) (Integer/parseInt))}))]
    (>!! exchange-chan order)
    )
  )

(defn dispatch-msg [msg]
  (let [msg-class (.getClass msg)]
    (cond
      (= msg-class NewOrderSingle) (new-order msg)
      ))
  )

(def application
  (reify quickfix.Application
    (onCreate [this session]
      (println session))
    (onLogon [this session])
    (onLogout [this session])
    (toApp [this message session] 
      (println "toApp" message)
      ) 
    (fromApp [this message session] 
      (println "fromApp" message)
      (dispatch-msg message)
      )
    (toAdmin [this message session])
    (fromAdmin [this message session])
    ))

; add order to book
(defn add-new-order [order]
  (fn [b]
    (let [s (order :side)
          {:keys [price quantity id]} order]
      (>!! execution-chan [order s 0.0 quantity])
      [order (update-in b [s] #(assoc % order [price quantity]))]
  )))

(defn execute-side [quantity s]
  (fn [book]
    (let [side (book s)
          [best [best-price best-quantity]] (peek side)
          remaining-quantity (- best-quantity quantity)
          _ (>!! execution-chan [best s best-price remaining-quantity])
          new-side (if (= quantity best-quantity)
                     (pop side)
                     (update-in side [best] (fn [[p q]] [p (- q quantity)])))]
      [nil (assoc book s new-side)])))

(defn execute [quantity]
  (domonad state-m [_ (execute-side quantity 1)
                    _ (execute-side quantity 2)] false)
  )

(defn match-book [_] 
  (fn [book]
  (let [buy (book 1)
        sell (book 2)
        [best-buy [best-buy-price buy-quantity]] (peek buy)
        [best-sell [best-sell-price sell-quantity]] (peek sell)
        match (and best-buy-price best-sell-price (>= best-buy-price best-sell-price))]
    (if match
      (let [quantity (min buy-quantity sell-quantity)]
        ((execute quantity) book)
        )
      [true book]))))

(defn match-until []
  (monads/with-monad state-m (monads/m-until identity match-book false)) 
  )

(defn handle-new-order [order]
  (domonad state-m [_ (add-new-order order)
                    _ (match-until)] nil))

(defn -main [config-path] 
  (let [settings (SessionSettings. config-path)
        storeFactory (FileStoreFactory. settings)
        logFactory (FileLogFactory. settings)
        messageFactory (DefaultMessageFactory.)
        acceptor (SocketAcceptor. application storeFactory settings logFactory messageFactory)]
    (.start acceptor))
  )
