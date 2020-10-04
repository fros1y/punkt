{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLists #-}
module NLP.Punkt where

import Control.Applicative ((<|>))
import Control.Lens ( (&), (^.), (%~) )
import qualified Data.HashMap.Strict as HM
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Control.Monad.Reader as Reader
import Data.Char (isAlpha, isLower, isSpace)
import Data.Generics.Labels ()
import Data.Generics.Product.Fields ()
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
import Data.Maybe (catMaybes, fromJust, fromMaybe)
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text as Text
import GHC.Generics ( Generic )
import NLP.Punkt.Match (re_split_pos, word_seps)
import Streamly (SerialT)
import qualified Streamly as S
import qualified Streamly.Data.Fold as FL
import qualified Streamly.Internal.Data.Fold as FL
import qualified Streamly.Prelude as S
import System.IO (Handle)

-- | Carries various orthographic statistics for a particular textual type.
data OrthoFreq = OrthoFreq
  { -- | number of lowercase occurrences
    freqLower :: !Int,
    -- | uppercase occurrences
    freqUpper :: !Int,
    -- | number of lowercase occurrences in the first position of a sentence
    freqFirstLower :: !Int,
    -- | number of uppercase occurrences strictly internal to a sentence
    freqInternalUpper :: !Int,
    -- | number of occurences in the first position
    freqAfterEnder :: !Int
  }
  deriving (Show, Generic, Store)

-- | Represents training data obtained from a corpus required by Punkt.
data PunktData = PunktData
  { -- | Occurrences of each textual type, case-insensitive. Used during Punkt's
    -- type-based stage. Also contains occurrences of trailing periods.
    typeCount :: !(HashMap Text Int),
    -- | Dictionary of orthographic data for each textual type.
    orthoCount :: !(HashMap Text OrthoFreq),
    collocations :: !(HashMap (Text, Text) Int),
    totalEnders :: !Int,
    totalToks :: !Int
  }
  deriving (Show, Generic, Store)

prunePunktData :: PunktData -> PunktData
prunePunktData punktData = punktData &
                              #typeCount %~ pruneHashMap &
                              #orthoCount %~ pruneOrthoCount &
                              #collocations %~ pruneHashMap where
  pruneHashMap = HM.filter (> 1)
  pruneOrthoCount = HM.filter validOrtho
  validOrtho ortho = 0 < ortho ^. #freqLower + ortho ^. #freqUpper + ortho ^. #freqFirstLower + ortho ^. #freqInternalUpper + ortho ^. #freqAfterEnder
 

data Entity a = Word !a Bool | Punct !a | ParaStart | Ellipsis | Dash
  deriving (Eq, Show, Generic, Store)

data Token = Token
  { offset :: !Int,
    tokLen :: !Int,
    entity :: !(Entity Text),
    sentEnd :: !Bool,
    abbrev :: !Bool
  }
  deriving (Show, Generic, Store)

type Punkt = Reader.Reader PunktData

readCorpus :: (S.IsStream t, MonadIO m) => Handle -> Maybe Int -> t m Text
readCorpus handle limit = case limit of
  Nothing -> linesFromFile
  Just limit' -> S.take limit' $ linesFromFile
  where
    linesFromFile = S.fromHandle handle & S.filter (/= "") & S.map Text.pack -- & S.concatMap to_tokens  --S.repeatM (Text.hGetLine handle) & S.filter (/= "") & S.map to_tokens

notEmpty :: Token -> Bool
notEmpty token = case entity token of
  Word word _ -> word /= ""
  _ -> True

toTokens :: (S.IsStream t, Monad m) => t m Text -> t m Token
toTokens linesOfText = S.filter notEmpty $ S.concatMap (S.fromList . toTokens') linesOfText
  where
    toTokens' corpus = catMaybes . map (either tokWord addDelim) $ re_split_pos word_seps corpus
    tokWord (w, pos)
      | trim == "" = Nothing
      | otherwise = Just $ Token pos (len trim) (Word s period) False False
      where
        trim = Text.dropAround (`elem` symbols) w
          where
            symbols = ",:()[]{}“”’\"\')" :: String
        period = Text.last trim == '.'
        s = if period then Text.init trim else trim

    addDelim (delim, pos)
      | d `elem` ("—-" :: String) = Just $ Token pos (len delim) Dash False False
      | d `elem` (".…" :: String) = Just $ Token pos (len delim) Ellipsis False False
      | d `elem` (";!?" :: String) = Just $ Token pos (len delim) (Punct delim) True False
      | otherwise = Nothing
      where
        d = Text.head delim

    len = Text.length

asPercentage :: (Integral a1, Integral a2) => a1 -> a2 -> String
asPercentage a b = show percentageRep <> "%"
  where
    percentageRep :: Int
    percentageRep = (round $ decimalRep * 100)
    decimalRep :: Double
    decimalRep = fromIntegral a / fromIntegral b

progress :: MonadIO m => String -> Maybe Int -> FL.Fold m p Int
progress stage total = FL.mkFoldId logging (pure 0)
  where
    logging cnt _ = do
      let fractional = case total of
            Just total' -> "/" <> (show total') <> " (" <> asPercentage cnt total' <> ") "
            Nothing -> ""
          baseMessage = stage <> " " <> (show cnt)
          message = baseMessage <> fractional <> " tokens processed"
      when (cnt `mod` 1000 == 0) $ liftIO $ putStrLn message
      pure $ cnt + 1

buildPunktData :: MonadIO m => SerialT m Token -> m PunktData
buildPunktData toks = do
  (_, (typeCnt, (lengthToks, wordCount))) <- S.fold (FL.tee (progress "First pass" Nothing) (FL.tee buildTypeCount (FL.tee FL.length (FL.lfilter isWord FL.length)))) toks
  liftIO $ putStrLn $ "Collected " <> (show lengthToks) <> " tokens"
  let tempPunkt = PunktData typeCnt Map.empty Map.empty 0 lengthToks
      leader = Token 0 0 (Word " " False) True False
      refined = S.tap (progress "Second pass" (Just lengthToks)) $ S.map (runClassification tempPunkt) toks
      offsetTokens = S.zipWith (,) (leader `S.cons` refined) refined
  (orthoCnt, (collocs, nender)) <- S.fold (FL.tee buildOrthoCount (FL.tee buildCollocs calcNender)) offsetTokens
  pure $ PunktData typeCnt orthoCnt collocs nender wordCount

calcNender :: Monad m => FL.Fold m (Token, b) Int
calcNender = FL.lfilter (sentEnd . fst) FL.length

buildTypeCount :: Monad m => FL.Fold m Token (HashMap Text Int)
buildTypeCount = FL.mkPureId update initCount
  where
    initCount = Map.singleton "." 0

    update ctr (Token {entity = (Word w per)})
      | per = Map.adjust (+ 1) "." ctr_
      | otherwise = ctr_
      where
        ctr_ = Map.insertWith (+) wnorm 1 ctr
        wnorm = norm $ if per then w `Text.snoc` '.' else w
    update ctr _ = ctr

countTokens :: Monad m => FL.Fold m a Int
countTokens = FL.length

buildOrthoCount :: Monad m => FL.Fold m (Token, Token) (HashMap Text OrthoFreq)
buildOrthoCount = FL.mkPureId update Map.empty
  where
    update :: HashMap Text OrthoFreq -> (Token, Token) -> HashMap Text OrthoFreq
    update ctr (prev, Token {entity = (Word w _)}) = Map.insert wnorm wortho ctr
      where
        upd (OrthoFreq a b c d e) a' b' c' d' e' =
          OrthoFreq (a |+ a') (b |+ b') (c |+ c') (d |+ d') (e |+ e')
          where
            int |+ bool = if bool then int + 1 else int

        wortho =
          upd
            z
            lower
            (not lower)
            (first && lower)
            (internal && not lower)
            first
        z = Map.lookupDefault (OrthoFreq 0 0 0 0 0) wnorm ctr
        wnorm = norm w
        lower = isLower $ Text.head w
        first = sentEnd prev && not (isInitial prev)
        internal =
          not (sentEnd prev) && not (abbrev prev)
            && not (isInitial prev)
    update ctr _ = ctr

buildCollocs :: Monad m => FL.Fold m (Token, Token) (HashMap (Text, Text) Int)
buildCollocs = FL.mkPureId update Map.empty
  where
    update ctr (Token {entity = (Word u _)}, Token {entity = (Word v _)}) = Map.insertWith (+) (norm u, norm v) 1 ctr
    update ctr _ = ctr

runClassification :: PunktData -> Token -> Token
runClassification tempPunkt token = runPunkt tempPunkt $ classifyByType token

norm :: Text -> Text
norm = Text.toLower

isInitial :: Token -> Bool
isInitial (Token {entity = Word w True}) =
  Text.length w == 1 && isAlpha (Text.head w)
isInitial _ = False

isWord :: Token -> Bool
isWord tok = case entity tok of Word _ _ -> True; _ -> False

-- | Dunning log likelihood modified by Kiss/Strunk
strunkLog :: Double -> Double -> Double -> Double -> Double
strunkLog a b ab n = -2 * (null' - alt)
  where
    null' = ab * log p1 + (a - ab) * log (1 - p1)
    alt = ab * log p2 + (a - ab) * log (1 - p2)
    (p1, p2) = (b / n, 0.99)

-- | Dunning's original log likelihood
dunningLog :: Double -> Double -> Double -> Double -> Double
dunningLog a b ab n
  | b == 0 || ab == 0 = 0
  | otherwise = -2 * (s1 + s2 - s3 - s4)
  where
    (p0, p1, p2) = (b / n, ab / a, (b - ab) / (n - a))
    s1 = ab * log p0 + (a - ab) * log (1 - p0)
    s2 = (b - ab) * log p0 + (n - a - b + ab) * log (1 - p0)
    s3 = if a == ab then 0 else ab * log p1 + (a - ab) * log (1 - p1)
    s4 =
      if b == ab
        then 0
        else (b - ab) * log p2 + (n - a - b + ab) * log (1 - p2)

ask_type_count :: Punkt (HashMap Text Int)
ask_type_count = Reader.liftM typeCount Reader.ask

ask_total_toks :: Num a => Punkt a
ask_total_toks = Reader.liftM (fromIntegral . totalToks) Reader.ask

ask_total_enders :: Num a => Punkt a
ask_total_enders = Reader.liftM (fromIntegral . totalEnders) Reader.ask

ask_ortho :: Text -> Punkt OrthoFreq
ask_ortho w_ =
  return . Map.lookupDefault (OrthoFreq 0 0 0 0 0) (norm w_)
    =<< fmap orthoCount Reader.ask

ask_colloc :: Text -> Text -> Punkt Double
ask_colloc w0_ w1_ =
  return . fromIntegral . Map.lookupDefault 0 (norm w0_, norm w1_)
    =<< collocations <$> Reader.ask

-- | Occurrences of a textual type, strictly ignoring trailing period.
-- @c(w, ~.)@. Case-insensitive.
freq :: Text -> Punkt Double
freq w_ = ask_type_count >>= return . fromIntegral . Map.lookupDefault 0 w
  where
    w = norm w_

-- | Occurrences of a textual type with trailing period. @c(w, .)@.
-- Case-insensitive.
freq_snoc_dot :: Text -> Punkt Double
freq_snoc_dot w_ = freq wdot where wdot = w_ `Text.snoc` '.'

-- potential slowdown if ghc doesn't know that norm "." == "."

-- | @c(w) == c(w, .) + c(w, ~.)@. Case-insensitive.
freq_type :: Text -> Punkt Double
freq_type w_ = (+) <$> freq w_ <*> freq_snoc_dot w_

dlen :: Text -> Double
dlen = fromIntegral . Text.length

-- | Returns the log likelihood that (w_ `snoc` '.') is an abbreviation.
-- Case-insensitive.
prob_abbr :: Text -> Punkt Double
prob_abbr w_ =
  compensate =<< strunkLog <$> freq_type w_ <*> freq "."
    <*> freq_snoc_dot w_
    <*> ask_total_toks
  where
    compensate loglike = do
      f_penalty <- do
        p <- freq w_ -- c(w, ~.)
        return $ 1 / dlen (Text.filter (/= '.') w_) ** p
      return $ loglike * f_len * f_periods * f_penalty
    f_len = 1 / exp (dlen $ Text.filter (/= '.') w_)
    f_periods = 1 + dlen (Text.filter (== '.') w_)

-- | Decides if @w@ is a sentence ender based on its capitalization.
-- Case-insensitive.
decide_ortho :: Text -> Punkt (Maybe Bool)
decide_ortho word = ask_ortho word >>= return . decide' word
  where
    decide' w_ wortho
      | title && ever_lower && never_title_internal = Just True
      | lower && (ever_title || never_lower_start) = Just False
      | otherwise = Nothing
      where
        (lower, title) = (isLower $ Text.head w_, not lower)
        ever_lower = freqLower wortho > 0
        never_title_internal = freqInternalUpper wortho == 0
        ever_title = freqUpper wortho > 0
        never_lower_start = freqFirstLower wortho == 0

-- | Special orthographic heuristic for post-possible-initial tokens.
-- Case-insensitive.
decide_initial_ortho :: Text -> Punkt (Maybe Bool)
decide_initial_ortho w_ = do
  neverlower <- (== 0) . freqLower <$> ask_ortho w_
  orthosays <- decide_ortho w_
  return $ orthosays <|> if neverlower then Just False else Nothing

-- | Log likelihood that @w@ is a frequent sentence starter. Case-insensitive.
prob_starter :: Text -> Punkt Double
prob_starter w_ =
  dunningLog <$> ask_total_enders <*> freq_type w_
    <*> fafterend
    <*> ask_total_toks
  where
    fafterend = fromIntegral . freqAfterEnder <$> ask_ortho w_

-- | Computes the collocational likelihood of @w@ and @x@. Case-insensitive.
prob_colloc :: Text -> Text -> Punkt Double
prob_colloc w_ x_ =
  dunningLog <$> freq_type w_ <*> freq_type x_
    <*> ask_colloc w_ x_
    <*> ask_total_toks

classifyByType' :: PunktData -> Token -> Token
classifyByType' model a = runPunkt model (classifyByType a)

classifyByType :: Token -> Punkt Token
classifyByType tok@(Token {entity = (Word w True)}) = do
  p <- prob_abbr w
  return $ tok {abbrev = p >= 0.3, sentEnd = p < 0.3}
classifyByType tok = return tok

classifyByNext' :: PunktData -> Token -> Token -> Token
classifyByNext' model a b = runPunkt model (classifyByNext a b)

classifyByNext :: Token -> Token -> Punkt Token
classifyByNext this (Token _ _ (Word next _) _ _)
  | isInitial this = do
    let Word thisinitial _ = entity this
    colo <- prob_colloc thisinitial next
    startnext <- prob_starter next
    orthonext <- decide_initial_ortho next
    return $
      if (colo >= 7.88 && startnext < 30) || orthonext == Just False
        then this {abbrev = True, sentEnd = False}
        else this -- never reclassify as sentend
  | entity this == Ellipsis || abbrev this = do
    ortho_says <- decide_ortho next
    prob_says <- prob_starter next
    return $ case ortho_says of
      Nothing -> this {sentEnd = prob_says >= 30}
      Just bool -> this {sentEnd = bool}
classifyByNext this _ = return this

classifyTokens :: (S.MonadAsync m) => PunktData -> SerialT m Token -> SerialT m Token
classifyTokens model tokens = do
  let  
    classifiedByType = S.map (classifyByType' model) tokens
    classifiedByNext = S.zipWith (classifyByNext' model) classifiedByType (S.drop 1 classifiedByType)
    lastToken = S.sequence $ pure $ fromJust <$> S.last tokens
  classifiedByNext <> lastToken

sentenceTokenize :: (S.MonadAsync m) => PunktData -> Text -> SerialT m (Int, Int)
sentenceTokenize model text = slicesFrom endpairs 0 where
  tokens = toTokens (S.yield text)
  pairs_of xs = S.zipWith (,) xs $ S.drop 1 xs
  endpairs = S.filter (sentEnd . fst) . pairs_of $ classifyTokens model tokens

  slicesFrom :: (S.MonadAsync m) => SerialT m (Token, Token) -> Int -> SerialT m (Int, Int)
  slicesFrom tokenStream n = do
    splitStream <- S.sequence $ pure $ S.uncons tokenStream
    case splitStream of
      Nothing -> S.yield (n, Text.length text)
      Just ((endtok, nexttok), pairs) -> do
        let 
          endpos = offset endtok + tokLen endtok
          (end, n') = fromMaybe (endpos, endpos + 1) . matchSpaces $ substring text endpos (offset nexttok)
        (n, endpos + end) S..: slicesFrom pairs (endpos + n')

substring :: Text -> Int -> Int -> Text
substring c s e = Text.take (e - s) $ Text.drop s c

matchSpaces :: Text -> Maybe (Int, Int)
matchSpaces w =
  Text.findIndex isSpace w >>= \p ->
    case Text.break notSpace (Text.drop p w) of
      (spaces, _) -> Just (p, Text.length spaces + p)
  where
    notSpace = not . isSpace

runPunkt :: PunktData -> Punkt a -> a
runPunkt = flip Reader.runReader
