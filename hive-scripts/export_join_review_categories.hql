drop table reviews_categories;
create external table reviews_categories  
	(user_id int,
	review_id string,
	votescool int,
	business_id int,
	votesfunny int,
	stars int,
	date string,
	type string,
	votesuseful int,
	categories string)
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as textfile location '/user/admin/yelp_filtered_data/reviews_categories';

insert into table reviews_categories
select c.userid_dummy as user_id ,a.review_id,
a.votescool,
b.businessid_dummy as business_id,
a.votesfunny,
a.stars,
a.date,
a.type,
a.votesuseful,
b.categories as categories from reviews as a inner join business_hash as b on a.business_id=b.business_id
inner join user_filtered as c on c.user_id=a.user_id ;


