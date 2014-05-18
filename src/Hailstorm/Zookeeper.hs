module Hailstorm.Zookeeper where

import qualified Database.Zookeeper as ZK

data ZKOptions = ZKOptions {
    connectionString :: String
}

connectAndRegisterProcessor :: ZKOptions -> String -> (ZK.Zookeeper -> IO ()) -> IO ()
connectAndRegisterProcessor zkOpts processorId cb = 
    connect zkOpts (\zk -> do
        me <- ZK.create zk ("/living_processors/" ++ processorId) Nothing ZK.OpenAclUnsafe [ZK.Ephemeral] 
        case me of 
            Left e -> putStrLn $ "Error from zookeeper: "  ++ (show e)
            Right _ -> cb zk
    )

connect :: ZKOptions -> (ZK.Zookeeper -> IO ()) -> IO ()
connect opts cb = ZK.withZookeeper (connectionString opts) 10000 Nothing Nothing cb

initialize :: ZK.Zookeeper -> IO ()
initialize zk = do
    me <- ZK.create zk "/living_processors" Nothing ZK.OpenAclUnsafe []
    case me of 
        Left e -> putStrLn $ "Error from zookeeper: " ++ (show e)
        Right _ -> return ()

statusString :: ZK.Zookeeper -> IO (String)
statusString zk = do
    me <- ZK.getChildren zk "/living_processors" Nothing
    case me of
        Left e -> do putStrLn $ "Error from zookeeper: " ++ (show e); return ""
        Right p -> return $ "Living processors: " ++ (show p)
