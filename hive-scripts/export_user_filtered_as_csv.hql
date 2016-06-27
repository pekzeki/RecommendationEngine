drop table user_temp;
create external table user_temp (yelping_since string,
                                 complimentsplain int,
                                 review_count int ,
                                 friends string,
                                 complimentscute int,
                                 complimentswriter int,
                                 fans int ,
                                 complimentsnote int,
                                 type string,
                                 complimentshot int,
                                 complimentscool int ,
                                 complimentsprofile int,
                                 average_stars float ,
                                 complimentsmore int,
                                 elite string,
                                 name string,
                                 user_id string,
                                 votescool int,
                                 complimentslist int,
                                 votesfunny int,
                                 complimentsphotos int,
                                 complimentsfunny int,
                                 votesuseful int,
                                 userid_dummy int) 
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile location '/user/admin/yelp_filtered_data/user_csv';
insert into table user_temp select * from user_filtered;

