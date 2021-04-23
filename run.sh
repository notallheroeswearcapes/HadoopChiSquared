call mvn package
call hadoop fs -mkdir /chi_squared_01624856
call hadoop fs -mkdir /chi_squared_01624856/cachedFiles
call hadoop fs -put stopwords.txt /test/cachedFiles