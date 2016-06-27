drop table reviews_categories_w_cities;
create external table reviews_categories_w_cities  
	(user_id int,
	review_id string,
	business_id int,
	categories string,
    city string,
    stars int)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as textfile location '/user/admin/yelp_filtered_data/reviews_categories_w_cities';

insert into table reviews_categories_w_cities
select c.userid_dummy as user_id ,a.review_id,
b.businessid_dummy as business_id,
b.categories as categories,
b.city as city,
a.stars 
from reviews_gt1 as a inner join business_hash as b on a.business_id=b.business_id
inner join users_review_gt1 as c on c.user_id=a.user_id ;
