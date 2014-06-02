module Main (main) where

import Control.Applicative
import Options
import Hailstorm.Sample.WordCountSample
import Hailstorm.Sample.WordCountKafkaEmitter
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.ProcessorState
import Hailstorm.Logging
import Hailstorm.SnapshotStore.DirSnapshotStore
import Hailstorm.InputSource.KafkaSource
import System.FilePath
import System.Environment
import qualified Hailstorm.Runner as HSR

-- | Options for the main program.
data MainOptions = MainOptions 
    { optConnect :: String
    , optBrokerConnect :: String 
    , optKafkaTopic :: String
    } deriving (Show)

instance Options MainOptions where
    defineOptions = pure MainOptions
        <*> simpleOption "connect" "127.0.0.1:2181"
            "Zookeeper connection string."
        <*> simpleOption "broker" "localhost:9092"
            "Kafka Broker connection string"
        <*> simpleOption "topic" "test"
            "Kafka Topic"

-- | Options for subcommands. Empty since subcommands do not take any options.
data EmptyOptions = EmptyOptions {}
instance Options EmptyOptions where
    defineOptions = pure EmptyOptions

-- | For run_sample_emitter
data EmitOptions = EmitOptions
    { optEmitSleepMs :: Int
    , optPartition :: Int
    } deriving (Show)

instance Options EmitOptions where
    defineOptions = pure EmitOptions
        <*> simpleOption "sleep" 10
            "Sleep between emits (ms)."
        <*> simpleOption "partition" 0
            "Partition to emit to."

-- | Builds Zookeeper options from command line options.
zkOptionsFromMainOptions :: MainOptions -> ZKOptions
zkOptionsFromMainOptions mainOptions =
    ZKOptions { connectionString = optConnect mainOptions }

-- | Builds Kafka options from command line options
kafkaOptionsFromMainOptions :: MainOptions -> KafkaOptions
kafkaOptionsFromMainOptions mainOptions =
    KafkaOptions { brokerConnectionString = optBrokerConnect mainOptions
                 , topic = optKafkaTopic mainOptions
                 }

-- | Initializes Zookeeper infrastructure for Hailstorm.
zkInit :: MainOptions -> EmptyOptions -> [String] -> IO ()
zkInit mainOpts _ _ = initializeCluster (zkOptionsFromMainOptions mainOpts)

-- | Prints debug information from Zookeeper instance.
zkShow :: MainOptions -> EmptyOptions -> [String] -> IO ()
zkShow mainOpts _ _ =
    getDebugInfo (zkOptionsFromMainOptions mainOpts) >>= putStrLn

-- | Runs the sample scenario and topology.
runSample :: MainOptions -> EmptyOptions -> [String] -> IO ()
runSample mainOpts _ _ = do
    -- NOTE: Symlink data/test.txt to ~ for convenience (assuming you
    -- symlink hailstorm too)
    home <- getEnv "HOME"
    initializeLogging
    let store = DirSnapshotStore $ home </> "store"
    HSR.localRunner (zkOptionsFromMainOptions mainOpts) wordCountTopology
        (home </> "test.txt") "words" store

runSampleEmitter :: MainOptions -> EmitOptions -> [String] -> IO ()
runSampleEmitter mainOpts emitOpts _ = do
    home <- getEnv "HOME"
    initializeLogging
    emitLinesForever 
        (home </> "test.txt")
        (kafkaOptionsFromMainOptions mainOpts)
        (optPartition emitOpts)
        (optEmitSleepMs emitOpts)

-- | Main entry point.
main :: IO ()
main = runSubcommand
    [ subcommand "zk_init" zkInit
    , subcommand "zk_show" zkShow
    , subcommand "run_sample" runSample
    , subcommand "run_sample_emitter" runSampleEmitter
    ]
