hadoop fs -cat /user/admin/yelp_filtered_data/users_filtered.csv | sed -i "1icategory\tuser_id\ttf\tidf\ttfidf" | hadoop fs -put /user/admin/yelp_filtered_data/user_filtered.csv
