module Main where

import Control.Applicative
import Options

import qualified Hailstorm.Zookeeper as HSZK
import qualified Hailstorm.Runner as HSR

data MainOptions = MainOptions {
    optConnect :: String
}

instance Options MainOptions where
    defineOptions = pure MainOptions
        <*> simpleOption "connect" "127.0.0.1:2181"
            "Zookeeper connection string"

data EmptyOptions = EmptyOptions {}
instance Options EmptyOptions where defineOptions = pure EmptyOptions

zkOptionsFromMainOptions :: MainOptions -> HSZK.ZKOptions
zkOptionsFromMainOptions mainOptions = HSZK.ZKOptions {
    HSZK.connectionString = (optConnect mainOptions)
}


zkInit :: MainOptions -> EmptyOptions -> [String] -> IO ()
zkInit mainOpts _ _ = HSZK.connect (zkOptionsFromMainOptions mainOpts) connected
    where connected zk = do
            HSZK.initialize zk

zkShow :: MainOptions -> EmptyOptions -> [String] -> IO ()
zkShow mainOpts _ _ = HSZK.connect (zkOptionsFromMainOptions mainOpts) connected
    where connected zk = do
            s <- HSZK.statusString zk
            putStrLn s

sampleRunner :: MainOptions -> EmptyOptions -> [String] -> IO ()
sampleRunner mainOpts _ _ = 
    HSR.localRunner (zkOptionsFromMainOptions mainOpts) HSR.sampleTopology HSR.sampleAddFormula

main :: IO ()
main = runSubcommand
    [ subcommand "zk_init" zkInit
    , subcommand "zk_show" zkShow
    , subcommand "run_sample" sampleRunner
    ]

