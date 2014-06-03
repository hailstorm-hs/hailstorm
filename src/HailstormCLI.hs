module Main (main) where

import Control.Applicative
import Control.Monad
import Data.List.Split
import Data.Maybe
import Hailstorm.InputSource
import Hailstorm.InputSource.FileSource
import Hailstorm.InputSource.KafkaSource
import Hailstorm.Logging
import Hailstorm.Processor
import Hailstorm.Sample.WordCountKafkaEmitter
import Hailstorm.Sample.WordCountSample
import Hailstorm.SnapshotStore
import Hailstorm.SnapshotStore.DirSnapshotStore
import Hailstorm.Topology
import Hailstorm.Topology.HardcodedTopology
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.ProcessorState
import Options
import System.Directory
import System.FilePath
import qualified Hailstorm.Runner as HSR
import qualified Data.Foldable
import qualified Data.Map as Map

-- | Options for the main program.
data MainOptions = MainOptions
    { optConnect :: String
    , optBrokerConnect :: String
    , optKafkaTopic :: String
    , optKafkaTimeout :: Double
    , optUseKafka :: Bool
    , optStore :: FilePath
    , optFile :: FilePath
    } deriving (Show)

instance Options MainOptions where
    defineOptions = pure MainOptions
        <*> simpleOption "connect" "127.0.0.1:2181"
            "Zookeeper connection string."
        <*> simpleOption "broker" "localhost:9092"
            "Kafka Broker connection string"
        <*> simpleOption "topic" "test"
            "Kafka Topic"
        <*> simpleOption "kafka-timeout" 60
            "Standard kafka timeout (seconds)"
        <*> simpleOption "use-kafka" False
            "Use kafka as an input source"
        <*> simpleOption "store" ".store"
            "Directory to use as snapshot store (default: .store directory in current)"
        <*> defineOption optionType_string (\o -> o
            { optionLongFlags = ["file"]
            , optionShortFlags = "f"
            , optionDefault = ""
            , optionDescription = "Path to file to use as source for stream"
            })

-- | Options for subcommands. Empty since subcommands do not take any options.
data EmptyOptions = EmptyOptions {}
instance Options EmptyOptions where
    defineOptions = pure EmptyOptions

-- | For run_sample_emitter
data EmitOptions = EmitOptions
    { optEmitSleepMs :: Int
    , optBatchSize :: Int
    } deriving (Show)

instance Options EmitOptions where
    defineOptions = pure EmitOptions
        <*> simpleOption "sleep" 10
            "Sleep between emits (ms)."
        <*> simpleOption "batch-size" 50
            "Emit batch size"

-- | For run_processors
data RunProcessorOptions = RunProcessorOptions
    { optTopology :: String
    } deriving (Show)

instance Options RunProcessorOptions where
    defineOptions = pure RunProcessorOptions
        <*> simpleOption "topology" "word_count"
            "Topology to use"

-- | Builds Zookeeper options from command line options.
zkOptionsFromMainOptions :: MainOptions -> ZKOptions
zkOptionsFromMainOptions mainOptions =
    ZKOptions { connectionString = optConnect mainOptions }

-- | Builds Kafka options from command line options.
kafkaOptionsFromMainOptions :: MainOptions -> KafkaOptions
kafkaOptionsFromMainOptions mainOptions =
    KafkaOptions { brokerConnectionString = optBrokerConnect mainOptions
                 , topic = optKafkaTopic mainOptions
                 , defaultKafkaTimeout = round $ 1000 * optKafkaTimeout mainOptions
                 }

sampleTopologyMap :: Map.Map String HardcodedTopology
sampleTopologyMap = Map.fromList
    [("word_count", wordCountTopology)]

fullPath :: FilePath -> FilePath -> FilePath
fullPath homeDir s =
    case splitPath s of
        "~/":s' -> joinPath $ homeDir:s'
        ["~"]   -> homeDir
        _       -> s

getFullPath :: FilePath -> IO FilePath
getFullPath p = do
    homeDir <- getHomeDirectory
    return $ fullPath homeDir p

createSnapshotStore :: MainOptions -> IO DirSnapshotStore
createSnapshotStore mainOptions = do
    d <- getFullPath (optStore mainOptions)
    return $ DirSnapshotStore d

getStreamSourceFile :: MainOptions -> IO FilePath
getStreamSourceFile mainOptions = do
    let fp = optFile mainOptions
    if null fp
        then error "No stream source file specified (use the -f option)"
        else getFullPath fp

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
    store <- createSnapshotStore mainOpts

    if optUseKafka mainOpts
        then runWithSource (KafkaSource $ kafkaOptionsFromMainOptions mainOpts) store
        else do
            f <- getStreamSourceFile mainOpts
            runWithSource (FileSource [f]) store

  where
    runWithSource :: (InputSource s, SnapshotStore o) => s -> o -> IO ()
    runWithSource = HSR.localRunner
        (zkOptionsFromMainOptions mainOpts) wordCountTopology "words"

-- | Runs specific processors relative to a topology.
runProcessors :: MainOptions -> RunProcessorOptions -> [String] -> IO ()
runProcessors mainOpts processorOpts processorMatches = do
    let topologyStr = optTopology processorOpts
        topology = fromMaybe (error $ "Unsupported topology: " ++ topologyStr)
            (Map.lookup topologyStr sampleTopologyMap)
        pids = if null processorMatches
                   then error "No processors specified"
                   else procIds processorMatches
        zkOpts = zkOptionsFromMainOptions mainOpts
    store <- createSnapshotStore mainOpts

    forM_ pids $ \pid -> Data.Foldable.forM_ (checkProcessor topology pid) error

    if optUseKafka mainOpts
        then HSR.runProcessors zkOpts topology
            (KafkaSource $ kafkaOptionsFromMainOptions mainOpts) store pids
        else do
            fp <- getStreamSourceFile mainOpts
            HSR.runProcessors zkOpts topology (FileSource [fp]) store pids

    where
        procIds :: [String] -> [ProcessorId]
        procIds = map $
            \match -> case splitOn "-" match of
                          [pName, pInstanceStr] -> (pName, read pInstanceStr :: Int)
                          _ -> error $ "Processor " ++ show match ++ " not in name-instance format"

        checkProcessor :: Topology t => t -> ProcessorId -> Maybe String
        checkProcessor topology (pName, pInstance) =
            case lookupProcessor pName topology of
                Nothing ->
                    if pName == "negotiator" && pInstance == 0 then Nothing
                    else Just $ "No processor named " ++ show pName
                Just processor ->
                    if pInstance >= parallelism processor then
                        Just $ "No processor instance " ++ show pInstance ++ " for " ++ show pName
                    else Nothing

runSampleEmitter :: MainOptions -> EmitOptions -> [String] -> IO ()
runSampleEmitter mainOpts emitOpts _ = do
    fp <- getStreamSourceFile mainOpts
    initializeLogging
    emitLinesForever
        fp
        (kafkaOptionsFromMainOptions mainOpts)
        (optEmitSleepMs emitOpts)
        (optBatchSize emitOpts)

-- | Main entry point.
main :: IO ()
main = runSubcommand
    [ subcommand "zk_init" zkInit
    , subcommand "zk_show" zkShow
    , subcommand "run_processor" runProcessors
    , subcommand "run_processors" runProcessors
    , subcommand "run_sample" runSample
    , subcommand "run_sample_emitter" runSampleEmitter
    ]
