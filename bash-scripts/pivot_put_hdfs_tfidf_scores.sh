rm /home/hduser/final_tfidf_scores_pivot_transpose.csv
spark-submit --master spark://master-1.cluster:7077 /home/hduser/pivot.py
sudo -u hdfs hadoop fs -rm /user/admin/project/similarity/final_tfidf_scores_pivot_transpose.csv
hadoop fs -copyFromLocal /home/hduser/final_tfidf_scores_pivot_transpose.csv /user/admin/project/similarity/
