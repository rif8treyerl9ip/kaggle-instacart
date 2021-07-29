-- from https://www.kaggle.com/tkm2261/my-15th-solution-features-mainly-using-bigquery/data

Making User features¶

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



--------------------------------------------------------------------------------------------
ユーザベース特徴量作成

- コマンド備考
/*
bq query --max_rows 1  --allow_large_results --destination_table "instacart.user_fund" --flatten_results --replace
- max_rows=MAX or -n=MAX
テーブルデータの表示で出力する最大行数を示す整数。デフォルト値は 100 です。
- allow_large_results={true|false}
レガシー SQL クエリで大きいサイズの宛先テーブルを有効にするには、true に設定します。デフォルト値は false です。
- replace={true|false}
宛先テーブルをクエリ結果で上書きするには、true に設定します。既存のデータとスキーマはすべて消去されます。--destination_kms_key フラグを指定しない限り、Cloud KMS 鍵も削除されます。デフォルト値は false です。
- destination_table=TABLE
指定すると、クエリ結果が TABLE に保存されます。 TABLE は、PROJECT:DATASET.TABLE の形式で指定します。PROJECT を指定しない場合は、現在のプロジェクトを指定したものとみなされます。--destination_table フラグを指定しないと、クエリ結果は一時テーブルに保存されます。
プロジェクト修飾テーブル名
- レガシー SQL では、プロジェクト修飾名でテーブルをクエリするには、区切り文字としてコロン（:）を使用します。例:
[hoge:hoge.prior_datamart]
*/
- 解釈
/*
ユーザベース特徴量作成

count(1) as user_item_cnt, ユーザ×総商品購入数
EXACT_COUNT_DISTINCT(product_id) ユーザ×ユニーク商品数
EXACT_COUNT_DISTINCT(department_id) ユーザ×ユニークジャンル(HOUSEHOLD,FROZEN...)数
EXACT_COUNT_DISTINCT(aisle_id) ユーザ×ユニーク通路数
EXACT_COUNT_DISTINCT(order_id) ユーザ×ユニーク注文数=注文回数
EXACT_COUNT_DISTINCT(order_id) / count(1)  ユーザ×商品数

MAX(order_number) ユーザ×商品数

AVG(days_since_prior_order) ユーザ×前回購入日からの経過日数の平均値
MAX(days_since_prior_order) ユーザ×前回購入日からの経過日数の最大値
MIN(days_since_prior_order) ユーザ×前回購入日からの経過日数の最小値

MAX(order_hour_of_day) ユーザ×購入時刻の平均値
MIN(order_hour_of_day) ユーザ×購入時刻の最大値
AVG(order_hour_of_day) ユーザ×購入時刻の最小値

AVG(reordered) ユーザ×再注文確率
SUM(reordered) ユーザ×再注文回数
AVG(order_dow) ユーザ×購入曜日の平均=どの曜日に買うかの傾向値
*/

--------------------------------------------------------------------------------------------
bq query --max_rows 1  --allow_large_results --destination_table "instacart.user_fund" --flatten_results --replace
"
SELECT
  user_id,
  count(1) as user_item_cnt,
  EXACT_COUNT_DISTINCT(product_id) as user_prd_cnt,
  EXACT_COUNT_DISTINCT(department_id) as user_depart_cnt,
  EXACT_COUNT_DISTINCT(aisle_id) as user_aisle_cnt,
  EXACT_COUNT_DISTINCT(order_id) as user_order_cnt,
  EXACT_COUNT_DISTINCT(order_id) / count(1)  as user_order_rate,
  
  MAX(order_number) as max_order_number,

  AVG(days_since_prior_order) as avg_days_since_prior_order,
  MAX(days_since_prior_order) as max_days_since_prior_order,
  MIN(days_since_prior_order) as min_days_since_prior_order,
  
  MAX(order_hour_of_day) as max_order_hour_of_day,
  MIN(order_hour_of_day) as min_order_hour_of_day,
  AVG(order_hour_of_day) as avg_order_hour_of_day,
  
  AVG(reordered) as avg_reordered,
  SUM(reordered) as sum_reordered,
  AVG(order_dow) as avg_order_dow
FROM
    [hoge:hoge.prior_datamart]
GROUP BY
  user_id
"

--------------------------------------------------------------------------------------------
bq query --max_rows 1  --allow_large_results --destination_table "instacart.user_freq" --flatten_results --replace
"
SELECT
  user_id,
  count(1) as order_cnt,
  AVG(days_since_prior_order) as avg_days_since_prior_order,
  MAX(days_since_prior_order) as max_days_since_prior_order,
  MIN(days_since_prior_order) as min_days_since_prior_order,
FROM
  [hoge:hoge.prior_datamart]
WHERE
  eval_set = 'prior'
GROUP BY
  user_id
"

--------------------------------------------------------------------------------------------
- 解釈
/*
sum(CASE WHEN order_dow = 0  THEN 1 ELSE 0 END) AS  order_dow_0,
...
sum(CASE WHEN order_dow = 6  THEN 1 ELSE 0 END) AS  order_dow_6,

特定の曜日でしか買い物をしない客をみつける特徴量、決定木でなければ要補完、このnullを活かすアイデアはすごい。
avg(CASE WHEN order_dow = 0  THEN reordered ELSE null END) AS  reorder_dow_0,
...
avg(CASE WHEN order_dow = 6  THEN reordered ELSE null END) AS  reorder_dow_6


*/

bq query --max_rows 1  --allow_large_results --destination_table "instacart.user_dow" --flatten_results --replace 
"
SELECT
  user_id,
  sum(CASE WHEN order_dow = 0  THEN 1 ELSE 0 END) AS  order_dow_0,
  sum(CASE WHEN order_dow = 1  THEN 1 ELSE 0 END) AS  order_dow_1,
  sum(CASE WHEN order_dow = 2  THEN 1 ELSE 0 END) AS  order_dow_2,
  sum(CASE WHEN order_dow = 3  THEN 1 ELSE 0 END) AS  order_dow_3,
  sum(CASE WHEN order_dow = 4  THEN 1 ELSE 0 END) AS  order_dow_4,
  sum(CASE WHEN order_dow = 5  THEN 1 ELSE 0 END) AS  order_dow_5,
  sum(CASE WHEN order_dow = 6  THEN 1 ELSE 0 END) AS  order_dow_6,

  avg(CASE WHEN order_dow = 0  THEN reordered ELSE null END) AS  reorder_dow_0,
  avg(CASE WHEN order_dow = 1  THEN reordered ELSE null END) AS  reorder_dow_1,
  avg(CASE WHEN order_dow = 2  THEN reordered ELSE null END) AS  reorder_dow_2,
  avg(CASE WHEN order_dow = 3  THEN reordered ELSE null END) AS  reorder_dow_3,
  avg(CASE WHEN order_dow = 4  THEN reordered ELSE null END) AS  reorder_dow_4,
  avg(CASE WHEN order_dow = 5  THEN reordered ELSE null END) AS  reorder_dow_5,
  avg(CASE WHEN order_dow = 6  THEN reordered ELSE null END) AS  reorder_dow_6
FROM
  [hoge:hoge.prior_datamart]
GROUP BY
  user_id
"

--------------------------------------------------------------------------------------------

bq query --max_rows 1  --allow_large_results --destination_table "instacart.user_hour" --flatten_results --replace
"
SELECT
  user_id,
  sum(CASE WHEN order_hour_of_day = 0  THEN 1 ELSE 0 END) AS order_hour_of_day_0,
  sum(CASE WHEN order_hour_of_day = 1  THEN 1 ELSE 0 END) AS order_hour_of_day_1,
  sum(CASE WHEN order_hour_of_day = 2  THEN 1 ELSE 0 END) AS order_hour_of_day_2,
  sum(CASE WHEN order_hour_of_day = 3  THEN 1 ELSE 0 END) AS order_hour_of_day_3,
  sum(CASE WHEN order_hour_of_day = 4  THEN 1 ELSE 0 END) AS order_hour_of_day_4,
  sum(CASE WHEN order_hour_of_day = 5  THEN 1 ELSE 0 END) AS order_hour_of_day_5,
  sum(CASE WHEN order_hour_of_day = 6  THEN 1 ELSE 0 END) AS order_hour_of_day_6,
  sum(CASE WHEN order_hour_of_day = 7  THEN 1 ELSE 0 END) AS order_hour_of_day_7,
  sum(CASE WHEN order_hour_of_day = 8  THEN 1 ELSE 0 END) AS order_hour_of_day_8,
  sum(CASE WHEN order_hour_of_day = 9  THEN 1 ELSE 0 END) AS order_hour_of_day_9,
  sum(CASE WHEN order_hour_of_day = 10  THEN 1 ELSE 0 END) AS order_hour_of_day_10,
  sum(CASE WHEN order_hour_of_day = 11  THEN 1 ELSE 0 END) AS order_hour_of_day_11,
  sum(CASE WHEN order_hour_of_day = 12  THEN 1 ELSE 0 END) AS order_hour_of_day_12,
  sum(CASE WHEN order_hour_of_day = 13  THEN 1 ELSE 0 END) AS order_hour_of_day_13,
  sum(CASE WHEN order_hour_of_day = 14  THEN 1 ELSE 0 END) AS order_hour_of_day_14,
  sum(CASE WHEN order_hour_of_day = 15  THEN 1 ELSE 0 END) AS order_hour_of_day_15,
  sum(CASE WHEN order_hour_of_day = 16  THEN 1 ELSE 0 END) AS order_hour_of_day_16,
  sum(CASE WHEN order_hour_of_day = 17  THEN 1 ELSE 0 END) AS order_hour_of_day_17,
  sum(CASE WHEN order_hour_of_day = 18  THEN 1 ELSE 0 END) AS order_hour_of_day_18,
  sum(CASE WHEN order_hour_of_day = 19  THEN 1 ELSE 0 END) AS order_hour_of_day_19,
  sum(CASE WHEN order_hour_of_day = 20  THEN 1 ELSE 0 END) AS order_hour_of_day_20,
  sum(CASE WHEN order_hour_of_day = 21  THEN 1 ELSE 0 END) AS order_hour_of_day_21,
  sum(CASE WHEN order_hour_of_day = 22  THEN 1 ELSE 0 END) AS order_hour_of_day_22,
  sum(CASE WHEN order_hour_of_day = 23  THEN 1 ELSE 0 END) AS order_hour_of_day_23
FROM
  `hoge.hoge.prior_datamart`
GROUP BY
  user_id
"
--------------------------------------------------------------------------------------------

bq query --max_rows 1  --allow_large_results --destination_table "instacart.user_depart" --flatten_results --replace "
"
SELECT
  user_id,
  sum(CASE WHEN department_id = 1  THEN 1 ELSE 0 END) AS department_id_1,
  sum(CASE WHEN department_id = 2  THEN 1 ELSE 0 END) AS department_id_2,
  sum(CASE WHEN department_id = 3  THEN 1 ELSE 0 END) AS department_id_3,
  sum(CASE WHEN department_id = 4  THEN 1 ELSE 0 END) AS department_id_4,
  sum(CASE WHEN department_id = 5  THEN 1 ELSE 0 END) AS department_id_5,
  sum(CASE WHEN department_id = 6  THEN 1 ELSE 0 END) AS department_id_6,
  sum(CASE WHEN department_id = 7  THEN 1 ELSE 0 END) AS department_id_7,
  sum(CASE WHEN department_id = 8  THEN 1 ELSE 0 END) AS department_id_8,
  sum(CASE WHEN department_id = 9  THEN 1 ELSE 0 END) AS department_id_9,
  sum(CASE WHEN department_id = 10  THEN 1 ELSE 0 END) AS department_id_10,
  sum(CASE WHEN department_id = 11  THEN 1 ELSE 0 END) AS department_id_11,
  sum(CASE WHEN department_id = 12  THEN 1 ELSE 0 END) AS department_id_12,
  sum(CASE WHEN department_id = 13  THEN 1 ELSE 0 END) AS department_id_13,
  sum(CASE WHEN department_id = 14  THEN 1 ELSE 0 END) AS department_id_14,
  sum(CASE WHEN department_id = 15  THEN 1 ELSE 0 END) AS department_id_15,
  sum(CASE WHEN department_id = 16  THEN 1 ELSE 0 END) AS department_id_16,
  sum(CASE WHEN department_id = 17  THEN 1 ELSE 0 END) AS department_id_17,
  sum(CASE WHEN department_id = 18  THEN 1 ELSE 0 END) AS department_id_18,
  sum(CASE WHEN department_id = 19  THEN 1 ELSE 0 END) AS department_id_19,
  sum(CASE WHEN department_id = 20  THEN 1 ELSE 0 END) AS department_id_20,
  sum(CASE WHEN department_id = 21  THEN 1 ELSE 0 END) AS department_id_21
FROM
  `hoge.hoge.train_datamart`
GROUP BY
  user_id
"

--------------------------------------------------------------------------------------------
bq query --max_rows 1  --allow_large_results --destination_table "instacart.dmt_user" --flatten_results --replace
"
SELECT
  *
FROM
  [instacart.user_fund] as u1
LEFT OUTER JOIN
  [instacart.user_dow] as u2
ON
  u1.user_id = u2.user_id
LEFT OUTER JOIN
  [instacart.user_hour] as u3
ON
  u1.user_id = u3.user_id
LEFT OUTER JOIN
  [instacart.user_depart] as u4
ON
  u1.user_id = u4.user_id
LEFT OUTER JOIN
  [instacart.user_freq] as u5
ON
  u1.user_id = u5.user_id
"
下のコードでないとGUIでは無理、重複列を自動で削除するのは--flatten_results
だろうか。これをGUI設定orjoin段階で重複削除できれば列を列挙せずに済む
-- prior_dmt_user
SELECT
    u1.user_id,
    user_item_cnt,
    user_prd_cnt,
    user_depart_cnt,
    user_aisle_cnt,
    user_order_cnt,
    user_order_rate,
    max_order_number,
    u1.avg_days_since_prior_order,
    u1.max_days_since_prior_order,
    u1.min_days_since_prior_order,
    max_order_hour_of_day,
    min_order_hour_of_day,
    avg_order_hour_of_day,
    avg_reordered,
    sum_reordered,
    avg_order_dow,
    order_dow_0,
    order_dow_1,
    order_dow_2,
    order_dow_3,
    order_dow_4,
    order_dow_5,
    order_dow_6,
    reorder_dow_0,
    reorder_dow_1,
    reorder_dow_2,
    reorder_dow_3,
    reorder_dow_4,
    reorder_dow_5,
    reorder_dow_6,
    order_hour_of_day_0,
    order_hour_of_day_1,
    order_hour_of_day_2,
    order_hour_of_day_3,
    order_hour_of_day_4,
    order_hour_of_day_5,
    order_hour_of_day_6,
    order_hour_of_day_7,
    order_hour_of_day_8,
    order_hour_of_day_9,
    order_hour_of_day_10,
    order_hour_of_day_11,
    order_hour_of_day_12,
    order_hour_of_day_13,
    order_hour_of_day_14,
    order_hour_of_day_15,
    order_hour_of_day_16,
    order_hour_of_day_17,
    order_hour_of_day_18,
    order_hour_of_day_19,
    order_hour_of_day_20,
    order_hour_of_day_21,
    order_hour_of_day_22,
    order_hour_of_day_23,
    department_id_1,
    department_id_2,
    department_id_3,
    department_id_4,
    department_id_5,
    department_id_6,
    department_id_7,
    department_id_8,
    department_id_9,
    department_id_10,
    department_id_11,
    department_id_12,
    department_id_13,
    department_id_14,
    department_id_15,
    department_id_16,
    department_id_17,
    department_id_18,
    department_id_19,
    department_id_20,
    department_id_21,
    order_cnt,
    -- avg_days_since_prior_order_1,
    -- max_days_since_prior_order_1,
    -- min_days_since_prior_order_1,
FROM
  `hoge.hoge.prior_user_features` AS u1
LEFT OUTER JOIN
  `hoge.hoge.prior_user_dow` AS u2
ON
  u1.user_id = u2.user_id
LEFT OUTER JOIN
  `hoge.hoge.prior_user_hour` AS u3
ON
  u1.user_id = u3.user_id
LEFT OUTER JOIN
  `hoge.hoge.prior_user_depart` AS u4
ON
  u1.user_id = u4.user_id
LEFT OUTER JOIN
  `hoge.hoge.prior_user_freq` AS u5
ON
  u1.user_id = u5.user_id