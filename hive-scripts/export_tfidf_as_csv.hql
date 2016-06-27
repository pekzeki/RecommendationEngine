set hive.io.output.fileformat = CSVTextFile;
set hive.exec.compress.output=false;
create table csv_dump ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' as
select * from tfidf;
