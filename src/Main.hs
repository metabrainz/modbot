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
import System.Log.Logger

import MusicBrainz
import MusicBrainz.Data.Edit
import MusicBrainz.Edit

processEdits :: Chan (Ref Edit) -> MusicBrainz ()
processEdits edits = do
  edit <- liftIO (readChan edits)
  liftIO $ debugM "ModBot." ("Attempting to close " ++ show edit)
  withTransaction $ apply edit

pollEdits :: Chan (Ref Edit) -> MusicBrainz ()
pollEdits edits = do
  liftIO $ debugM "ModBot.Poll" "Polling for edits to close"
  ready <- map fromOnly <$> findOpenEdits
  liftIO $ forM_ ready $ \editId -> writeChan edits editId
  where
    findOpenEdits = query [sql|
      SELECT edit_id
      FROM (
        SELECT
          edit_id,
          count(editor_id) AS editor_count,
          sum(vote) / (count(editor_id)::float) AS score
        FROM (
          SELECT
            edit_id, vote, editor_id,
            row_number() OVER (PARTITION BY editor_id ORDER BY vote_time DESC)
          FROM edit
          JOIN vote USING (edit_id)
          WHERE status = ?
        ) a
        WHERE row_number = 1
        GROUP BY edit_id
      ) b
      WHERE
        (editor_count >= 3 AND score >= (1.0/3.0)) OR
        (editor_count < 3 AND score = 1)
    |] (Only Open)

main :: IO ()
main = do
  updateGlobalLogger rootLoggerName (setLevel DEBUG)

  (config, _) <- autoReload autoConfig [Required "modbot.conf"]

  connectInfo <- parseConnectInfo config
  frequency <- lookupDefault (1 * 60) config "frequency"

  edits <- newChan

  void $ do
    forkIO $ runMb connectInfo (forever $ processEdits edits)

  runMb connectInfo $ forever $ do
    pollEdits edits
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
