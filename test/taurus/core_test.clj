(ns taurus.core-test
  (:require [clojure.test :refer :all]
            [taurus.core :refer :all])
  (:import [quickfix 
            Application SessionID Message SessionSettings FileStoreFactory FileLogFactory DefaultMessageFactory
            SocketAcceptor SocketInitiator Session]
           [quickfix.fix44 OrderCancelRequest NewOrderSingle ExecutionReport]
           [quickfix.field SenderCompID TargetCompID OrigClOrdID ClOrdID Symbol Side TransactTime OrdType OrderQty Price OrderID ExecID
            ExecType OrdStatus LeavesQty CumQty AvgPx]
           [java.io FileInputStream])
  )

(def clientapplication
  (reify quickfix.Application
    (onCreate [this session]
      (println session))
    (onLogon [this session])
    (onLogout [this session])
    (toApp [this message session] ) 
    (fromApp [this message session]) 
    (toAdmin [this message session])
    (fromAdmin [this message session])
    ))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))

(def connector (SocketInitiator. clientapplication storeFactory settings logFactory messageFactory))
(.start connector)

(let [msg1 (NewOrderSingle. (ClOrdID. "1") (Side. Side/SELL) (TransactTime. ) (OrdType. OrdType/MARKET))
      _ (doto msg1 
          (.set (Symbol. "IBM"))
          (.set (OrderQty. 5000))
          (.set (Price. 121.0)))
      msg2 (NewOrderSingle. (ClOrdID. "2") (Side. Side/SELL) (TransactTime. ) (OrdType. OrdType/MARKET))
                 _ (doto msg2
                     (.set (Symbol. "IBM"))
                     (.set (OrderQty. 5000))
                     (.set (Price. 122.0)))
                 msg3 (NewOrderSingle. (ClOrdID. "3") (Side. Side/BUY) (TransactTime. ) (OrdType. OrdType/MARKET))
                 _ (doto msg3
                     (.set (Symbol. "IBM"))
                     (.set (OrderQty. 7000))
                     (.set (Price. 122.0)))]
             (Session/sendToTarget msg1 "ROOK" "EXCHANGE")
             (Session/sendToTarget msg2 "ROOK" "EXCHANGE")
             (Session/sendToTarget msg3 "ROOK" "EXCHANGE")
  )
