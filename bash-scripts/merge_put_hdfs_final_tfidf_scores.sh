rm /home/hduser/final_tfidf_scores.csv
hadoop fs -getmerge /user/admin/project/tfidf/output/final_tfidf_scores/part* /home/hduser/final_tfidf_scores.csv 
rm /home/hduser/.*.crc
sed -i "1icategory\tuser_id\ttf\tidf\ttfidf" /home/hduser/final_tfidf_scores.csv 
sudo -u hdfs hadoop fs -rm /user/admin/project/tfidf/final_tfidf_scores.csv
hadoop fs -copyFromLocal /home/hduser/final_tfidf_scores.csv /user/admin/project/tfidf/
