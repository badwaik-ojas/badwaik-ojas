import org.apache.spark.sql.SparkSession

object SparkDataSourceCSV extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkDataSourceCSV")
    .getOrCreate()

  val peopleDF = spark
    .read
    .option("delimiter",";")
    .option("header","true")
    .csv("src/main/resources/people.csv")

  val peopleDF_1 = spark
    .read
    .options(Map("delimiter" -> ";","header" -> "true"))
    .csv("src/main/resources/people.csv")

  peopleDF_1.show()

  /*
  Different Options available:
  sep -> "," -> Sets a separator for each field and value. This separator can be one or more characters.
  encoding -> "UTF-8" -> For reading, decodes the CSV files by the given encoding type.
      For writing, specifies encoding (charset) of saved CSV files.
      CSV built-in functions ignore this option.
  quote -> " -> Sets a single character used for escaping quoted values where the separator
      can be part of the value. For reading, if you would like to turn off
      quotations, you need to set not null but an empty string.
  quoteAll -> "false" -> A flag indicating whether all values should always
      be enclosed in quotes. Default is to only escape values containing a
      quote character.
  escape -> "\" -> Sets a single character used for escaping quotes inside
      an already quoted value.
  escapeQuotes -> "true" -> A flag indicating whether values containing quotes
      should always be enclosed in quotes. Default is to escape all values
      containing a quote character.
  header -> "false" -> For reading, uses the first line as names of columns.
      For writing, writes the names of columns as the first line.
      Note that if the given path is a RDD of Strings, this header option will remove all lines same with the header if exists.
      CSV built-in functions ignore this option.
  inferSchema -> "false" -> Infers the input schema automatically from data. It requires one extra pass over the data.
      CSV built-in functions ignore this option.
  enforceSchema -> "true" -> If it is set to true, the specified or inferred schema will be forcibly applied to datasource files,
      and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in
      CSV files in the case when the header option is set to true.
      Field names in the schema and column names in CSV headers are checked by their positions taking into account
      spark.sql.caseSensitive.
  nullValue -> Sets the string representation of a null value.
  nanValue -> "NaN" -> Sets the string representation of a non-number value.
  mode -> "PERMISSIVE" or "DROPMALFORMED" or "FAILFAST" -> Allows a mode for dealing with corrupt records during parsing.
      It supports the following case-insensitive modes.
  lineSep -> "\n" or "\r" or "\r\n" -> Defines the line separator that should be used for parsing/writing. Maximum length is 1
      character. CSV built-in functions ignore this option.
  */

}
