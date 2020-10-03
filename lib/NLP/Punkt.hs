{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE DuplicateRecordFields #-}

module NLP.Punkt where

import qualified Data.Text as Text
import GHC.Generics
import Data.Monoid (Sum(..))
import Data.Function ((&))
import Data.Text (Text)
import Control.Monad.IO.Class ( MonadIO )
import Data.Store ( Store )
import Data.Maybe (catMaybes, fromMaybe, fromJust)
import Data.HashMap.Strict (HashMap, unionWith)
import Data.Char (isLower, isAlpha, isSpace)
import Control.Lens
import qualified Data.HashMap.Strict as Map
import Control.Applicative ((<|>))
import qualified Control.Monad.Reader as Reader
import Streamly (SerialT)
import System.IO (Handle)
import Data.Generics.Product.Fields
import Data.Generics.Labels

import qualified Streamly as S
import qualified Streamly.Data.Fold as FL
import qualified Streamly.Internal.Data.Fold as FL
import qualified Streamly.Prelude as S

import NLP.Punkt.Match (re_split_pos, word_seps)

-- | Carries various orthographic statistics for a particular textual type.
data OrthoFreq = OrthoFreq {
    freqLower :: !Int,
    -- ^ number of lowercase occurrences
    freqUpper :: !Int,
    -- ^ uppercase occurrences
    freqFirstLower :: !Int,
    -- ^ number of lowercase occurrences in the first position of a sentence
    freqInternalUpper :: !Int,
    -- ^ number of uppercase occurrences strictly internal to a sentence
    freqAfterEnder :: !Int
    -- ^ number of occurences in the first position
    }
    deriving (Show, Generic, Store)

instance Semigroup OrthoFreq where
    (<>) a b = OrthoFreq freqLower' freqUpper' freqFirstLower' freqInternalUpper' freqAfterEnder' where
        freqLower' = (a ^. #freqLower) + (b ^. #freqLower)
        freqUpper' = (a ^. #freqUpper) + (b ^. #freqUpper)
        freqFirstLower' = (a ^. #freqFirstLower) + (b ^. #freqFirstLower)
        freqInternalUpper' = (a ^. #freqInternalUpper) + (b ^. #freqInternalUpper)
        freqAfterEnder' = (a ^. #freqAfterEnder) + (b ^. #freqAfterEnder)

-- | Represents training data obtained from a corpus required by Punkt.
data PunktData = PunktData {
    typeCount :: !(HashMap Text Int),
    -- ^ Occurrences of each textual type, case-insensitive. Used during Punkt's
    -- type-based stage. Also contains occurrences of trailing periods.
    orthoCount :: !(HashMap Text OrthoFreq),
    -- ^ Dictionary of orthographic data for each textual type.
    collocations :: !(HashMap (Text, Text) Int),
    totalEnders :: !Int,
    totalToks :: !Int
    }
    deriving (Show, Generic, Store)

instance Semigroup PunktData where
    (<>) punktA punktB = PunktData typeCount' orthoCount' collocations' totalEnders' totalToks' where
        totalToks' = punktA ^. #totalToks + punktB ^. #totalToks
        totalEnders' = punktA ^. #totalEnders + punktB ^. #totalEnders
        collocations' = unionWith (+) (punktA ^. #collocations) (punktB ^. #collocations)
        typeCount' = unionWith (+) (punktA ^. #typeCount) (punktB ^. #typeCount)
        orthoCount' = unionWith (<>) (punktA ^. #orthoCount) (punktB ^. #orthoCount)


data Entity a = Word !a Bool | Punct !a | ParaStart | Ellipsis | Dash
    deriving (Eq, Show, Generic, Store)

data Token = Token {
    offset :: !Int,
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
toTokens linesOfText = S.filter notEmpty $ S.concatMap (S.fromList . toTokens') linesOfText where
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
        where  d = Text.head delim

    len = Text.length



countWords :: Monad m => FL.Fold m Token (Sum Int)
countWords = FL.foldMap counter where
    counter :: Token -> Sum Int
    counter t = if isWord t then (Sum 1) else (Sum 0)

buildPunktData :: Monad m => SerialT m Token -> m PunktData
buildPunktData toks = do
    (typeCnt, (lengthToks, wordCount)) <- S.fold (FL.tee buildTypeCount (FL.tee FL.length countWords)) toks
    let tempPunkt = PunktData typeCnt Map.empty Map.empty 0 lengthToks
        leader = Token 0 0 (Word " " False) True False
        refined = S.map (runClassification tempPunkt) toks
        offsetTokens = S.zipWith (,) (leader `S.cons` refined) refined
    (orthoCnt, (collocs, nender)) <- S.fold (FL.tee buildOrthoCount (FL.tee buildCollocs calcNender)) offsetTokens
    pure $ PunktData typeCnt orthoCnt collocs nender (getSum wordCount)

calcNender :: Monad m => FL.Fold m (Token, b) Int
calcNender = FL.lfilter (sentEnd . fst) FL.length

newtype TypeCount = TypeCount {unwrap :: HashMap Text Int}
instance Semigroup TypeCount where
    (<>) (TypeCount a) (TypeCount b) = TypeCount $ unionWith (+) a b
instance Monoid TypeCount where
    mempty = TypeCount $ Map.singleton "." 0


buildTypeCount :: Monad m => FL.Fold m Token (HashMap Text Int)
buildTypeCount = unwrap <$> FL.foldMap buildTypeCount' where
    buildTypeCount' (Token {entity=(Word w per)}) = TypeCount hashMap where 
            hashMap = Map.adjust (+ 1) "." $ Map.insertWith (+) wnorm 1 mempty 
            wnorm = norm $ if per then w `Text.snoc` '.' else w
    buildTypeCount' _ = mempty

countTokens :: Monad m => FL.Fold m a Int
countTokens = FL.length

newtype OrthoCount = OrthoCount {unOrtho :: HashMap Text OrthoFreq}
instance Semigroup OrthoCount where
    (<>) (OrthoCount a) (OrthoCount b) = OrthoCount $ unionWith (<>) a b
instance Monoid OrthoCount where
    mempty = OrthoCount mempty

--buildOrthoCount :: Monad m => SerialT m Token -> m (HashMap Text OrthoFreq)
buildOrthoCount :: Monad m => FL.Fold m (Token, Token) (HashMap Text OrthoFreq)
buildOrthoCount = FL.mkPureId update Map.empty 
    where
    update :: HashMap Text OrthoFreq -> (Token, Token) -> HashMap Text OrthoFreq
    update ctr (prev, Token {entity=(Word w _)}) = Map.insert wnorm wortho ctr
        where
        upd (OrthoFreq a b c d e) a' b' c' d' e' =
            OrthoFreq (a |+ a') (b |+ b') (c |+ c') (d |+ d') (e |+ e')
            where int |+ bool = if bool then int + 1 else int

        wortho = upd z lower (not lower) (first && lower)
                       (internal && not lower) first
        z = Map.lookupDefault (OrthoFreq 0 0 0 0 0) wnorm ctr
        wnorm = norm w
        lower = isLower $ Text.head w
        first = sentEnd prev && not (isInitial prev)
        internal = not (sentEnd prev) && not (abbrev prev)
                   && not (isInitial prev)
    update ctr _ = ctr

--buildCollocs :: (Monad m, Num v) => SerialT m Token -> m (HashMap (Text, Text) v)
buildCollocs :: Monad m => FL.Fold m (Token, Token) (HashMap (Text, Text) Int)
buildCollocs = FL.mkPureId update Map.empty where
        update ctr (Token {entity=(Word u _)}, Token {entity=(Word v _)}) = Map.insertWith (+) (norm u, norm v) 1 ctr
        update ctr _ = ctr

runClassification :: PunktData -> Token -> Token
runClassification tempPunkt token = runPunkt tempPunkt $ classify_by_type token


norm :: Text -> Text
norm = Text.toLower

isInitial :: Token -> Bool
isInitial (Token {entity=Word w True}) =
    Text.length w == 1 && isAlpha (Text.head w)
isInitial _ = False

isWord :: Token -> Bool
isWord tok = case entity tok of { Word _ _ -> True; _ -> False; }

-- | Dunning log likelihood modified by Kiss/Strunk
strunkLog :: Double -> Double -> Double -> Double -> Double
strunkLog a b ab n = -2 * (null - alt)
    where
    null = ab * log p1 + (a - ab) * log (1 - p1)
    alt = ab * log p2 + (a - ab) * log (1 - p2)
    (p1, p2) = (b / n, 0.99)

-- | Dunning's original log likelihood
dunningLog :: Double -> Double -> Double -> Double -> Double
dunningLog a b ab n | b == 0 || ab == 0 = 0
                     | otherwise = -2 * (s1 + s2 - s3 - s4)
    where
    (p0, p1, p2) = (b / n, ab / a, (b - ab) / (n - a))
    s1 = ab * log p0 + (a - ab) * log (1 - p0)
    s2 = (b - ab) * log p0 + (n - a - b + ab) * log (1 - p0)
    s3 = if a == ab then 0 else ab * log p1 + (a - ab) * log (1 - p1)
    s4 = if b == ab then 0 else
        (b - ab) * log p2 + (n - a - b + ab) * log (1 - p2)

ask_type_count :: Punkt (HashMap Text Int)
ask_type_count = Reader.liftM typeCount Reader.ask

ask_total_toks :: Num a => Punkt a
ask_total_toks = Reader.liftM (fromIntegral . totalToks) Reader.ask

ask_total_enders :: Num a => Punkt a
ask_total_enders = Reader.liftM (fromIntegral . totalEnders) Reader.ask

ask_ortho :: Text -> Punkt OrthoFreq
ask_ortho w_ = return . Map.lookupDefault (OrthoFreq 0 0 0 0 0) (norm w_)
               =<< fmap orthoCount Reader.ask

ask_colloc :: Text -> Text -> Punkt Double
ask_colloc w0_ w1_ =
    return . fromIntegral . Map.lookupDefault 0 (norm w0_, norm w1_)
    =<< collocations <$> Reader.ask

-- | Occurrences of a textual type, strictly ignoring trailing period.
-- @c(w, ~.)@. Case-insensitive.
freq :: Text -> Punkt Double
freq w_ = ask_type_count >>= return . fromIntegral . Map.lookupDefault 0 w
    where w = norm w_

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
prob_abbr w_ = compensate =<< strunkLog <$> freq_type w_ <*> freq "."
                                         <*> freq_snoc_dot w_ <*> ask_total_toks
    where
    compensate loglike = do
        f_penalty <- do
            p <- freq w_  -- c(w, ~.)
            return $ 1 / dlen (Text.filter (/= '.') w_) ** p
        return $ loglike * f_len * f_periods * f_penalty
    f_len = 1 / exp (dlen $ Text.filter (/= '.') w_)
    f_periods = 1 + dlen (Text.filter (== '.') w_)

-- | Decides if @w@ is a sentence ender based on its capitalization.
-- Case-insensitive.
decide_ortho :: Text -> Punkt (Maybe Bool)
decide_ortho w_ = ask_ortho w_ >>= return . decide' w_
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
prob_starter w_ = dunningLog <$> ask_total_enders <*> freq_type w_
                              <*> fafterend <*> ask_total_toks
    where fafterend = fromIntegral . freqAfterEnder <$> ask_ortho w_

-- | Computes the collocational likelihood of @w@ and @x@. Case-insensitive.
prob_colloc :: Text -> Text -> Punkt Double
prob_colloc w_ x_ = dunningLog <$> freq_type w_ <*> freq_type x_
                                <*> ask_colloc w_ x_ <*> ask_total_toks


classify_by_type :: Token -> Punkt Token
classify_by_type tok@(Token {entity=(Word w True)}) = do
    p <- prob_abbr w
    return $ tok { abbrev = p >= 0.3, sentEnd = p < 0.3}
classify_by_type tok = return tok

classify_by_next :: Token -> Token -> Punkt Token
classify_by_next this (Token _ _ (Word next _) _ _)
    | isInitial this = do
        let Word thisinitial _ = entity this
        colo <- prob_colloc thisinitial next
        startnext <- prob_starter next
        orthonext <- decide_initial_ortho next
        return $ if (colo >= 7.88 && startnext < 30) || orthonext == Just False
            then this { abbrev = True, sentEnd = False}
            else this  -- never reclassify as sentend
    | entity this == Ellipsis || abbrev this = do
        ortho_says <- decide_ortho next
        prob_says <- prob_starter next
        return $ case ortho_says of
            Nothing -> this { sentEnd = prob_says >= 30 }
            Just bool -> this { sentEnd = bool }
classify_by_next this _ = return this

-- classify_punkt :: PunktData -> Text -> [Token]
-- classify_punkt model corpus = runPunkt model $ do
--     abbrd <- mapM classify_by_type toks
--     final <- Reader.zipWithM classify_by_next abbrd (drop 1 abbrd)
--     return $ final ++ [last toks]
--     where toks = to_tokens corpus

-- find_breaks :: PunktData -> Text -> [(Int, Int)]
-- find_breaks model corpus = slices_from endpairs 0
--     where
--     pairs_of xs = zip xs $ drop 1 xs
--     endpairs = filter (sentEnd . fst) . pairs_of $ classify_punkt model corpus

--     -- TODO: make this less convoluted
--     slices_from [] n = [(n, Text.length corpus)]
--     slices_from ((endtok, nexttok):pairs) n = (n, endpos + end) : slices_from pairs (endpos + n')
--         where
--         endpos = offset endtok + tokLen endtok
--         (end, n') = fromMaybe (endpos, endpos + 1) . match_spaces $
--             substring corpus endpos (offset nexttok)

substring :: Text -> Int -> Int -> Text
substring c s e = Text.take (e - s) $ Text.drop s c

match_spaces :: Text -> Maybe (Int, Int)
match_spaces w = Text.findIndex isSpace w >>= \p ->
    case Text.break notSpace (Text.drop p w) of
        (spaces, _) -> Just (p, Text.length spaces + p)
    where notSpace = not . isSpace

-- | Main export of the entire package. Splits a corpus into its constituent
-- sentences.
-- split_sentences :: PunktData -> Text -> [Text]
-- split_sentences model corpus = map (uncurry $ substring corpus) slices
--     where slices = find_breaks model corpus

-- | @runPunkt data computation@ runs @computation@ using @data@ collected from
-- a corpus using 'build_punkt_data'.
runPunkt :: PunktData -> Punkt a -> a
runPunkt = flip Reader.runReader
