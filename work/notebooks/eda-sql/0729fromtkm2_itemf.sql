-- from https://www.kaggle.com/tkm2261/my-15th-solution-features-mainly-using-bigquery/data

Making item features
-- 基本標準、だめならレガシーで

--------------------------------------------------------------------------------------------
- 解釈
/*
count(1) 商品×ユーザ数
EXACT_COUNT_DISTINCT(user_id) ある商品を買ったことがあるユーザ
EXACT_COUNT_DISTINCT(department_id) 商品×ジャンル数、order by descしても最大値1だが、複数ジャンルの可能性を考えてEDA的に実行したのか？
EXACT_COUNT_DISTINCT(aisle_id) 商品×通路数、order by descしても最大値1だが、複数ジャンルの可能性を考えてEDA的に実行したのか？
EXACT_COUNT_DISTINCT(order_id) 商品が累計何個注文されたか、これがEXACT_COUNT_DISTINCT(user_id)とまったく一致しているのだが、order_idのユニーク数のほうがuser_idにユニーク数より多い可能性が高いので、ふつう異なった値になりそう。何か解釈間違えがあるのか。
EXACT_COUNT_DISTINCT(order_id) / count(1) 全注文数に対する特定の商品の注文比率  
AVG(days_since_prior_order) 商品×前回の注文からの平均経過日数
MIN(days_since_prior_order) 商品×前回の注文からの最小経過日数
MAX(days_since_prior_order) 商品×前回の注文からの最大経過日数
MAX(order_hour_of_day) 商品×注文時刻
MIN(order_hour_of_day) 商品×注文時刻
AVG(order_hour_of_day) 商品×注文時刻
AVG(reordered) 商品×再注文確率
SUM(reordered) 商品×何回再注文されたか
AVG(order_dow) 商品×注文された曜日の傾向
*/

bq query --max_rows 1  --allow_large_results --destination_table "instacart.item_fund" --flatten_results --replace "  
SELECT
  product_id,
  count(1) as item_user_cnt,
  EXACT_COUNT_DISTINCT(user_id) as item_usr_cnt,
  EXACT_COUNT_DISTINCT(department_id) as item_depart_cnt,
  EXACT_COUNT_DISTINCT(aisle_id) as item_aisle_cnt,
  EXACT_COUNT_DISTINCT(order_id) as item_order_cnt,
  EXACT_COUNT_DISTINCT(order_id) / count(1) as item_order_rate,
  AVG(days_since_prior_order) as avg_item_days_since_prior_order,
  MIN(days_since_prior_order) as min_item_days_since_prior_order,
  MAX(days_since_prior_order) as max_item_days_since_prior_order,
  MAX(order_hour_of_day) as max_order_hour_of_day,
  MIN(order_hour_of_day) as min_order_hour_of_day,
  AVG(order_hour_of_day) as avg_order_hour_of_day,
  AVG(reordered) as avg_item_reordered,
  SUM(reordered) as sum_item_reordered,
  AVG(order_dow) as avg_order_dow
FROM
  [the-capsule-321000:15th_solution_features.train_datamart]
GROUP BY
  product_id
"

--------------------------------------------------------------------------------------------
- 解釈
/*
商品ごとに何曜日に買われたかの足し合わせ
*/

bq query --max_rows 1  --allow_large_results --destination_table "instacart.item_dow" --flatten_results --replace "  
SELECT
  product_id,
  sum(CASE WHEN order_dow = 0  THEN 1 ELSE 0 END) AS  order_dow_0,
  sum(CASE WHEN order_dow = 1  THEN 1 ELSE 0 END) AS  order_dow_1,
  sum(CASE WHEN order_dow = 2  THEN 1 ELSE 0 END) AS  order_dow_2,
  sum(CASE WHEN order_dow = 3  THEN 1 ELSE 0 END) AS  order_dow_3,
  sum(CASE WHEN order_dow = 4  THEN 1 ELSE 0 END) AS  order_dow_4,
  sum(CASE WHEN order_dow = 5  THEN 1 ELSE 0 END) AS  order_dow_5,
  sum(CASE WHEN order_dow = 6  THEN 1 ELSE 0 END) AS  order_dow_6
FROM
  `the-capsule-321000.15th_solution_features.prior_datamart`
GROUP BY
  product_id
"

--------------------------------------------------------------------------------------------
- 解釈
/*
商品ごとに何時に買われたかの足し合わせ
*/
bq query --max_rows 1  --allow_large_results --destination_table "instacart.item_hour" --flatten_results --replace "  
SELECT
  product_id,
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
  `the-capsule-321000.15th_solution_features.prior_datamart`
GROUP BY
  product_id
"

--------------------------------------------------------------------------------------------
- 解釈
/*
このスパース行列から何を得たいのか不明。モデルはこれをどう解釈するのか。どう活かすのか気になる
*/
bq query --max_rows 1  --allow_large_results --destination_table "instacart.item_depart" --flatten_results --replace "  
SELECT
  product_id,
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
  `the-capsule-321000.15th_solution_features.prior_datamart`
GROUP BY
  product_id
"

--------------------------------------------------------------------------------------------
- 解釈
/*
*/

bq query --max_rows 1  --allow_large_results --destination_table "instacart.dmt_item" --flatten_results --replace "  
SELECT
  *
FROM
  [instacart.item_fund] as i1
LEFT OUTER JOIN
  [instacart.item_dow] as i2
ON
  i1.product_id = i2.product_id
LEFT OUTER JOIN
  [instacart.item_hour] as i3
ON
  i1.product_id = i3.product_id
LEFT OUTER JOIN
  [instacart.item_depart] as i4
ON
  i1.product_id = i4.product_id
"
標準に書き換えたやつ↓
-- train_dmt_item
SELECT
i1.product_id,
item_user_cnt,
item_usr_cnt,
item_depart_cnt,
item_aisle_cnt,
item_order_cnt,
item_order_rate,
avg_item_days_since_prior_order,
min_item_days_since_prior_order,
max_item_days_since_prior_order,
max_order_hour_of_day,
min_order_hour_of_day,
avg_order_hour_of_day,
avg_item_reordered,
sum_item_reordered,
avg_order_dow,
order_dow_0,
order_dow_1,
order_dow_2,
order_dow_3,
order_dow_4,
order_dow_5,
order_dow_6,
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
department_id_21
FROM
    `the-capsule-321000.15th_solution_features.train_item_features` as i1
LEFT OUTER JOIN
    `the-capsule-321000.15th_solution_features.train_item_dow` as i2
ON
  i1.product_id = i2.product_id
LEFT OUTER JOIN
    `the-capsule-321000.15th_solution_features.train_item_hour` as i3
ON
  i1.product_id = i3.product_id
LEFT OUTER JOIN
    `the-capsule-321000.15th_solution_features.train_item_depart` as i4
ON
  i1.product_id = i4.product_id


