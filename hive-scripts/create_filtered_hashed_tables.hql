drop table user_filtered;
create table user_filtered as select s.*,hash(aaa.users) as userid_dummy from users as s inner join (select  user_id as Users, count(distinct(review_id)) as review_number  from reviews  group by user_id having review_number>1 order by review_number ASC) as aaa
on s.user_id=aaa.users;
drop table business_filtered;
create table business_filtered as select a.*, hash(business_id) as businessid_dummy from businesses as a;
drop table review_filtered;
create table review_filtered as select r.*, u.userid_dummy from reviews as r inner join users_review_gt1 as u  on r.user_id=u.user_id;
