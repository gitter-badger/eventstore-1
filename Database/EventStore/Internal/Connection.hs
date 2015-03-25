{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE ScopedTypeVariables #-}
--------------------------------------------------------------------------------
-- |
-- Module : Database.EventStore.Internal.Connection
-- Copyright : (C) 2015 Yorick Laupa
-- License : (see the file LICENSE)
--
-- Maintainer : Yorick Laupa <yo.eight@gmail.com>
-- Stability : provisional
-- Portability : non-portable
--
--------------------------------------------------------------------------------
module Database.EventStore.Internal.Connection
    ( Connection
    , ConnectionException(..)
    , connUUID
    , connClose
    , connFlush
    , connSend
    , connRecv
    , newConnection
    ) where

--------------------------------------------------------------------------------
import           Control.Concurrent
import           Control.Exception
import qualified Data.ByteString as B
import           Data.Typeable
import           System.IO
import           Text.Printf

--------------------------------------------------------------------------------
import           Data.Default.Class
import           Data.UUID
import           Network
import qualified Network.Connection as TLS
import           System.Random

--------------------------------------------------------------------------------
import Database.EventStore.Internal.Types

--------------------------------------------------------------------------------
data ConnectionException =
    MaxAttempt HostName Int Int -- ^ HostName Port MaxAttempt's value
    deriving (Show, Typeable)

--------------------------------------------------------------------------------
instance Exception ConnectionException

--------------------------------------------------------------------------------
data Connection =
    Connection
    { connUUID  :: UUID
    , connClose :: IO ()
    , connFlush :: IO ()
    , connSend  :: B.ByteString -> IO ()
    , connRecv  :: Int -> IO B.ByteString
    }

--------------------------------------------------------------------------------
newConnection :: Settings -> IO Connection
newConnection sett =
    case s_retry sett of
        AtMost n ->
            let loop i = do
                    printf "Connecting...Attempt %d\n" i
                    catch (connect sett) $ \(_ :: SomeException) -> do
                        threadDelay delay
                        let host = s_hostname sett
                            port = s_port sett
                        if n <= i then throwIO $ MaxAttempt host port n
                                  else loop (i + 1) in
             loop 1
        KeepRetrying ->
            let endlessly i = do
                    printf "Connecting...Attempt %d\n" i
                    catch (connect sett) $ \(_ :: SomeException) -> do
                        threadDelay delay >> endlessly (i + 1) in
             endlessly (1 :: Int)
  where
    delay = (s_reconnect_delay_secs sett) * secs

--------------------------------------------------------------------------------
secs :: Int
secs = 1000000

--------------------------------------------------------------------------------
connect :: Settings -> IO Connection
connect sett =
    case s_connectionType sett of
        Uncrypted -> regularConnection sett
        Encrypted -> encryptedConnection sett

--------------------------------------------------------------------------------
regularConnection :: Settings -> IO Connection
regularConnection sett = do
    hdl <- connectTo host (PortNumber $ fromIntegral port)
    hSetBuffering hdl NoBuffering
    uuid <- randomIO
    return Connection
           { connUUID  = uuid
           , connClose = hClose hdl
           , connFlush = hFlush hdl
           , connSend  = B.hPut hdl
           , connRecv  = B.hGet hdl
           }
  where
    host = s_hostname sett
    port = s_port sett

--------------------------------------------------------------------------------
encryptedConnection :: Settings -> IO Connection
encryptedConnection sett = do
    ctx  <- TLS.initConnectionContext
    conn <- TLS.connectTo ctx connParams
    uuid <- randomIO
    return Connection
           { connUUID  = uuid
           , connClose = TLS.connectionClose conn
           , connFlush = return ()
           , connSend  = TLS.connectionPut conn
           , connRecv  = TLS.connectionGet conn
           }
  where
    host = s_hostname sett
    port = s_port sett

    tsimple =
      def
      { TLS.settingDisableCertificateValidation = True }

    connParams =
        TLS.ConnectionParams
        { TLS.connectionHostname  = host
        , TLS.connectionPort      = fromIntegral port
        , TLS.connectionUseSecure = Just tsimple
        , TLS.connectionUseSocks  = Nothing
        }
