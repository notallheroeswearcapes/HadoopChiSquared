#!/bin/sh
DATA_PATH="hdfs:///user/pknees/amazon-reviews/full/reviews_devset.json"
OUTPUT_PATH="hdfs:///tmp/e1624856_DIC2021_Ex1/"
echo "Please refer to README for additional information"
echo "Creating directories and building the project..."
rm -rf target/DIC_Ex1_01624856-1.0-shaded.jar target/DIC_Ex1_01624856-1.0.jar
rm -rf e1624856_results
mkdir e1624856_results
mvn clean package -Dskiptests
hadoop fs -rm -r ${OUTPUT_PATH}
hadoop fs -mkdir ${OUTPUT_PATH}
hadoop fs -mkdir ${OUTPUT_PATH}chi_squared
hadoop fs -mkdir ${OUTPUT_PATH}chi_squared/cached_files
hadoop fs -put stopwords.txt ${OUTPUT_PATH}/chi_squared/cached_files/
echo "Running MapReduce jobs..."
hadoop jar target/DIC_Ex1_01624856-1.0-shaded.jar Driver ${DATA_PATH} ${OUTPUT_PATH}out_first ${OUTPUT_PATH}out_second ${OUTPUT_PATH}out_chi_squared ${OUTPUT_PATH}out_dict ${OUTPUT_PATH}chi_squared/cached_files/stopwords.txt
echo "MapReduce jobs finished"
echo "Fetching output..."
cd e1624856_results
hadoop fs -get ${OUTPUT_PATH}out_chi_squared/chi-squared-third-r-00000 output_chi_squared.txt
hadoop fs -get ${OUTPUT_PATH}out_dict/chi-squared-fourth-r-00000 output_dict.txt
cat output_chi_squared.txt output_dict.txt > output.txt
echo "Output fetched"
echo "You can find the output files in the directory 'e1624856_results/' in the project root"