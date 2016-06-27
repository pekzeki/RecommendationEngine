drop table TFIDF;
create external table TFIDF (
  word string,
  user_id string,
  tf double,
  idf double,
  tfidf double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
location '/user/admin/project/tfidf/output/final_tfidf_scores';
