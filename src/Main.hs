{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
module Main where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad
import Control.Monad.IO.Class
import Data.Configurator
import Database.PostgreSQL.Simple (Only(..))
import Database.PostgreSQL.Simple.SqlQQ (sql)

import MusicBrainz
import MusicBrainz.Edit

pollEdits :: Chan (Ref Edit) -> MusicBrainz ()
pollEdits edits = do
  edit <- liftIO (readChan edits)
  return ()

processEdits :: Chan (Ref Edit) -> MusicBrainz ()
processEdits edits = do
  ready <- map fromOnly <$> findOpenEdits
  liftIO $ forM_ ready $ \editId -> writeChan edits editId
  where
    findOpenEdits = query [sql| SELECT * FROM edit WHERE status = ? |]
      (Only Open)

main :: IO ()
main = do
  (config, _) <- autoReload autoConfig [Required "modbot.conf"]

  connectInfo <- parseConnectInfo config
  frequency <- lookupDefault (1 * 60) config "frequency"

  edits <- newChan

  void $ do
    forkIO $ runMb connectInfo (pollEdits edits)

  runMb connectInfo $ forever $ do
    processEdits edits
    liftIO $ threadDelay (frequency * 1000000)

  where
    parseConnectInfo config =
      ConnectInfo
        <$> dbSetting connectHost "host"
        <*> dbSetting connectPort "port"
        <*> dbSetting connectUser "user"
        <*> dbSetting connectPassword "password"
        <*> dbSetting connectDatabase "database"
      where
        dbSetting def name =
          lookupDefault (def defaultConnectInfo) config name
