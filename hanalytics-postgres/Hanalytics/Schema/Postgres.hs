{-|
Module: Hanalytics.Schema.Postgres
Description: PostgreSQL schema definitions and deriving.
License: MIT
-}

{-# LANGUAGE FlexibleContexts, FlexibleInstances, DefaultSignatures, LambdaCase, OverloadedStrings, TypeOperators, ViewPatterns #-}

module Hanalytics.Schema.Postgres
	( postgresSchemaFields
	, postgresSqlCreateType
	, postgresSqlInsertGroup
	, postgresSqlUpsertGroup
	, ToPostgresText(..)
	) where

import qualified Data.Aeson as J
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import Data.Int
import Data.Proxy
import Data.Scientific
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TLB
import qualified Data.Text.Lazy.Builder.Int as TLB
import qualified Data.Text.Lazy.Builder.RealFloat as TLB
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.Vector as V
import qualified GHC.Generics as G

import Hanalytics.Schema

-- | Gets a list of record specifications.
postgresSchemaFields :: Bool -> Schema -> V.Vector TLB.Builder
postgresSchemaFields constraintsAllowed Schema
	{ schema_fields = fields
	} = V.map (postgresSchemaField constraintsAllowed) fields

postgresSchemaField :: Bool -> SchemaField -> TLB.Builder
postgresSchemaField constraintsAllowed SchemaField
	{ schemaField_mode = fieldMode
	, schemaField_name = fieldName
	, schemaField_type = fieldType
	} = let
	fieldTypeConstraints = case fieldMode of
		SchemaFieldMode_required -> if constraintsAllowed then " NOT NULL" else ""
		SchemaFieldMode_optional -> ""
		SchemaFieldMode_repeated -> " ARRAY" <> (if constraintsAllowed then " NOT NULL" else "")
	in "\"" <> TLB.fromText fieldName <> "\" " <> postgresSchemaFieldTypeName fieldType <> fieldTypeConstraints

postgresSchemaFieldTypeName :: SchemaFieldType -> TLB.Builder
postgresSchemaFieldTypeName = \case
	SchemaFieldType_bytes -> "BYTEA"
	SchemaFieldType_string -> "TEXT"
	SchemaFieldType_int64 -> "BIGINT"
	SchemaFieldType_integer -> "NUMERIC"
	SchemaFieldType_rational -> "NUMERIC"
	SchemaFieldType_float -> "DOUBLE PRECISION"
	SchemaFieldType_bool -> "BOOLEAN"
	SchemaFieldType_json -> "JSON"
	SchemaFieldType_record
		{ schemaFieldType_schema = Schema
			{ schema_name = n
			}
		} -> TLB.fromText $ "\"" <> n <> "\""

-- | Generate SQL CREATE TYPE command.
postgresSqlCreateType :: Schema -> TLB.Builder
postgresSqlCreateType schema@Schema
	{ schema_name = name
	} = "CREATE TYPE \"" <> TLB.fromText name <> "\" AS (" <> foldr1Comma (postgresSchemaFields False schema) <> ");\n"

-- | Generate SQL INSERT command for a bunch of records.
postgresSqlInsertGroup :: (Schemable a, ToPostgresText a) => T.Text -> [a] -> TLB.Builder
postgresSqlInsertGroup tableName records@(schemaOfListElement -> Schema
	{ schema_fields = fields
	}) = "INSERT INTO \"" <> TLB.fromText tableName <> "\"(" <> fieldsText <> ") VALUES " <> recordsText <> ";\n" where
	recordsText = foldr1Comma $ map (\a -> "(" <> toPostgresText True a <> ")") records
	fieldsText = foldr1Comma $ fmap (\SchemaField
		{ schemaField_name = fieldName
		} -> "\"" <> TLB.fromText fieldName <> "\"") fields

-- | Generate SQL upsert (INSERT ... ON CONFLICT DO UPDATE) command for a bunch of records.
postgresSqlUpsertGroup :: (Schemable a, ToPostgresText a) => T.Text -> T.Text -> [a] -> TLB.Builder
postgresSqlUpsertGroup conflictField tableName records@(schemaOfListElement -> Schema
	{ schema_fields = fields
	}) = "INSERT INTO \"" <> TLB.fromText tableName <> "\"(" <> fieldsText <> ") VALUES " <> recordsText <> " ON CONFLICT (\"" <> TLB.fromText conflictField <> "\") DO UPDATE SET " <> assignsText <> ";\n" where
	recordsText = foldr1Comma $ map (\a -> "(" <> toPostgresText True a <> ")") records
	fieldsText = foldr1Comma $ fmap (\SchemaField
		{ schemaField_name = fieldName
		} -> "\"" <> TLB.fromText fieldName <> "\"") fields
	assignsText = foldr1Comma $ fmap (\SchemaField
		{ schemaField_name = fieldName
		} -> "\"" <> TLB.fromText fieldName <> "\" = EXCLUDED.\"" <> TLB.fromText fieldName <> "\"") fields

schemaOfListElement :: Schemable a => [a] -> Schema
schemaOfListElement = schemaOf . f where
	f :: [a] -> Proxy a
	f _ = Proxy

-- | Class for exporting values into postgres text import format.
class ToPostgresText a where
	toPostgresText :: Bool -> a -> TLB.Builder
	default toPostgresText :: (G.Generic a, GenericToPostgresTextDatatype (G.Rep a)) => Bool -> a -> TLB.Builder
	toPostgresText inline = genericToPostgresTextDatatype inline . G.from

class GenericToPostgresTextDatatype f where
	genericToPostgresTextDatatype :: Bool -> f p -> TLB.Builder

class GenericToPostgresTextConstructor f where
	genericToPostgresTextConstructor :: Bool -> f p -> TLB.Builder

class GenericToPostgresTextSelector f where
	genericToPostgresTextSelector :: f p -> TLB.Builder

class GenericToPostgresTextValue f where
	genericToPostgresTextValue :: f p -> TLB.Builder

instance (G.Datatype c, GenericToPostgresTextConstructor f) => GenericToPostgresTextDatatype (G.M1 G.D c f) where
	genericToPostgresTextDatatype inline = genericToPostgresTextConstructor inline . G.unM1

instance (G.Constructor c, GenericToPostgresTextSelector f) => GenericToPostgresTextConstructor (G.M1 G.C c f) where
	genericToPostgresTextConstructor inline = wrapIfNotInline inline . genericToPostgresTextSelector . G.unM1

instance (G.Selector c, GenericToPostgresTextValue f) => GenericToPostgresTextSelector (G.M1 G.S c f) where
	genericToPostgresTextSelector = genericToPostgresTextValue . G.unM1
instance (GenericToPostgresTextSelector a, GenericToPostgresTextSelector b) => GenericToPostgresTextSelector (a G.:*: b) where
	genericToPostgresTextSelector (a G.:*: b) = genericToPostgresTextSelector a <> ", " <> genericToPostgresTextSelector b

instance ToPostgresText a => GenericToPostgresTextValue (G.K1 G.R a) where
	genericToPostgresTextValue = toPostgresText False . G.unK1

-- some instances

instance ToPostgresText T.Text where
	toPostgresText _ s = TLB.fromText $ "'" <> T.replace "'" "''" s <> "'"

instance ToPostgresText TL.Text where
	toPostgresText _ s = TLB.fromLazyText $ "'" <> TL.replace "'" "''" s <> "'"

instance ToPostgresText B.ByteString where
	toPostgresText _ bytes = "E'\\\\x" <> TLB.fromText (T.decodeUtf8 $ BA.convertToBase BA.Base16 bytes) <> "'"

instance ToPostgresText Int64 where
	toPostgresText _ = TLB.decimal

instance ToPostgresText Integer where
	toPostgresText _ = TLB.decimal

instance ToPostgresText Float where
	toPostgresText _ = TLB.realFloat

instance ToPostgresText Double where
	toPostgresText _ = TLB.realFloat

instance ToPostgresText Scientific where
	toPostgresText _ = TLB.fromString . show

instance ToPostgresText Bool where
	toPostgresText _ b = if b then "TRUE" else "FALSE"

instance ToPostgresText J.Value where
	toPostgresText _ = toPostgresText False . TL.decodeUtf8 . J.encode

instance ToPostgresText a => ToPostgresText (Maybe a) where
	toPostgresText _ = maybe "NULL" (toPostgresText False)

instance (ToPostgresText a, SchemableField a) => ToPostgresText (V.Vector a) where
	toPostgresText _ v@(V.map (toPostgresText False) -> vt) = (if V.null v then "'{}'" else "ARRAY[" <> foldr1Comma vt <> "]") <> "::" <> elementTypeStr v Proxy <> "[]" where
		elementTypeStr :: SchemableField a => V.Vector a -> Proxy (V.Vector a) -> TLB.Builder
		elementTypeStr _ = postgresSchemaFieldTypeName . schemaFieldTypeOf

wrapIfNotInline :: Bool -> TLB.Builder -> TLB.Builder
wrapIfNotInline inline = if inline then id else wrapRow

wrapRow :: TLB.Builder -> TLB.Builder
wrapRow a = "ROW(" <> a <> ")"

foldr1Comma :: (Foldable f, Monoid a, IsString a) => f a -> a
foldr1Comma = foldr1 $ \a b -> a <> ", " <> b
