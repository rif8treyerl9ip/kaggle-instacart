TRANSCODE py sql

Making my datamart ( joinining all data into one table)
-- train datamart
[py]
df_train = pd.merge(order_products_train_df, orders_df, how='left', on='order_id')
df_train = pd.merge(df_train, products_df, how='left', on='product_id')
df_train = pd.merge(df_train, aisles_df, how='left', on='aisle_id')
df_train = pd.merge(df_train, departments_df, how='left', on='department_id')
#df_train.to_csv('../input/df_train.csv', index=False) # if you want to use my feature, plz comment out.
df_train.head()

[sql]
-- Making my train datamart
select
tr.order_id,
tr.product_id,
tr.add_to_cart_order,
tr.reordered,
-- order_id_1,
ord.user_id,
ord.eval_set,
ord.order_number,
ord.order_dow,
ord.order_hour_of_day,
ord.days_since_prior_order,
-- product_id_1,
p.product_name,
p.aisle_id,
p.department_id,
-- aisle_id_1,
a.aisle,
-- department_id_1,
d.department
from `hoge.kaggle_insta.order_products__train` as tr
inner join `hoge.kaggle_insta.orders` as ord
on tr.order_id = ord.order_id
inner join `hoge.kaggle_insta.products` as p
on tr.product_id = p.product_id
inner join `hoge.kaggle_insta.aisles` as a
on p.aisle_id = a.aisle_id
inner join `hoge.kaggle_insta.departments` as d
on p.department_id = d.department_id

-- prior datamart
[py]
df_prior = pd.merge(order_products_prior_df, orders_df, how='left', on='order_id').head(10000) 
# if you want to use my feature, plz remove the ".head(10000)".
df_prior = pd.merge(df_prior, products_df, how='left', on='product_id')
df_prior = pd.merge(df_prior, aisles_df, how='left', on='aisle_id')
df_prior = pd.merge(df_prior, departments_df, how='left', on='department_id')
#df_prior.to_csv('../input/df_prior.csv', index=False) # if you want to use my feature, plz comment out.


[sql]
-- Making my prior datamart
select
pr.order_id,
pr.product_id,
pr.add_to_cart_order,
pr.reordered,
-- order_id_1,
ord.user_id,
ord.eval_set,
ord.order_number,
ord.order_dow,
ord.order_hour_of_day,
ord.days_since_prior_order,
-- product_id_1,
p.product_name,
p.aisle_id,
p.department_id,
-- aisle_id_1,
a.aisle,
-- department_id_1,
d.department
from `hoge.kaggle_insta.order_products__prior` as pr
inner join `hoge.kaggle_insta.orders` as ord
on pr.order_id = ord.order_id
inner join `hoge.kaggle_insta.products` as p
on pr.product_id = p.product_id
inner join `hoge.kaggle_insta.aisles` as a
on p.aisle_id = a.aisle_id
inner join `hoge.kaggle_insta.departments` as d
on p.department_id = d.department_id








---------------------------------ここから下はフリースペース---------------------------------
-- warehouse


各列のnullの行を数えるには、order_idなど主キーを指定(多分)して、whereでnullの数を数えたい列を指定
select count(order_id)
select count(*)
from `hoge.kaggle_insta.orders`
where days_since_prior_order is NULL

これをcount(days_since_prior_order)としてしまうと0が返される、nullはカウントされない
select count(days_since_prior_order)
from `hoge.kaggle_insta.orders`
where days_since_prior_order is NULL

これが恐らくベストプラクティス
select
sum(case when days_since_prior_order is null then 1 else 0 end) as nullcount1
-- sum(case when col2 is null then 1 else 0 end) as nullcount2,
from `hoge.kaggle_insta.orders`







COUNTは全列に対して適用するとNULLを数える
select COUNT(*)
from `hoge.kaggle_insta.orders`

COUNTは各列に対して適用するとNULLを数えない
select
count(days_since_prior_order)
from `hoge.kaggle_insta.orders`