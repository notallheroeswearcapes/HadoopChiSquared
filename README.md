Dear DIC-Team,

please execute the file `run.sh` by typing `bash run.sh` from the project root to run my program. The development version 
of the Amazon Review dataset is used as the default. If you want to use the full dataset, you need to change the 
variable `DATA_PATH` in the `run.sh` file. The output of the jobs will be saved in the directory `e1624856_DIC2021_Ex1` 
which resides in the `tmp` directory on HDFS. The resulting files including the `output.txt` file will be automatically
fetched from HDFS and put in the new directory `e1624856_results` in the project root. The other two files in this
folder are the chi squared results and the merged dictionary. The remaining intermediate result files can be found in 
the output directories on HDFS in `hdfs:///tmp/e1624856_DIC2021_Ex1/`.

Also, thank you for extending the deadline!

Matthias Eder, 01624856