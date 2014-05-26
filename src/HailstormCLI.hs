module Main (main) where

import Control.Applicative
import Options
import Hailstorm.Sample.WordCountSample
import Hailstorm.ZKCluster
import Hailstorm.Logging
import System.FilePath
import System.Environment
import qualified Hailstorm.Runner as HSR

-- | Options for the main program.
data MainOptions = MainOptions { optConnect :: String }
instance Options MainOptions where
    defineOptions = pure MainOptions
        <*> simpleOption "connect" "127.0.0.1:2181"
            "Zookeeper connection string."

-- | Options for subcommands. Empty since subcommands do not take any options.
data EmptyOptions = EmptyOptions {}
instance Options EmptyOptions where
    defineOptions = pure EmptyOptions

-- | Builds Zookeeper options from command line options.
zkOptionsFromMainOptions :: MainOptions -> ZKOptions
zkOptionsFromMainOptions mainOptions =
    ZKOptions { connectionString = optConnect mainOptions }

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
    HSR.localRunner (zkOptionsFromMainOptions mainOpts) wordCountTopology
        wordCountFormula  (home </> "test.txt") "words"

-- | Main entry point.
main :: IO ()
main = runSubcommand
    [ subcommand "zk_init" zkInit
    , subcommand "zk_show" zkShow
    , subcommand "run_sample" runSample
    ]
