#!/bin/sh
DATA_PATH = hdfs:///user/pknees/amazon-reviews/full/reviews_devset.json
OUTPUT_PATH = hdfs:///tmp/e1624856_DIC2021_Ex1/

rm -rf target/DIC_Ex1_01624856-1.0-shaded.jar target/DIC_Ex1_01624856-1.0.jar
rm -rf e1624856_results
mkdir e1624856_results
mvn clean package
hadoop fs -rm -r OUTPUT_PATH
hadoop fs -mkdir OUTPUT_PATH/chi_squared
hadoop fs -mkdir OUTPUT_PATH/chi_squared/cachedFiles
hadoop fs -put stopwords.txt OUTPUT_PATH/chi_squared/cachedFiles
hadoop jar target/DIC_Ex1_01624856-1.0-shaded.jar Driver $DATA_PATH OUTPUT_PATH/out_first OUTPUT_PATH/out_second OUTPUT_PATH/out_chi_squared OUTPUT_PATH/out_dict OUTPUT_PATH/chi_squared/cached_files/stopwords.txt
cd e1624856_results
hadoop fs -get $OUTPUT_PATH/out_chi_squared/chi-squared-third-r-00000 output_chi_squared.txt
hadoop fs -get $OUTPUT_PATH/out_dict/chi-squared-fourth-r-00000 output_dict.txt
cat output_chi_squared.txt output_dict.txt > output.txt