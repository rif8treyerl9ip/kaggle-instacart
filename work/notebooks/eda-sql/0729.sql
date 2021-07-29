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




EXACT_COUNT_DISTINCT:恐らくNULLを除外したユニークカウント
SELECT
  EXACT_COUNT_DISTINCT(reordered)
FROM
  [the-capsule-321000:15th_solution_features.prior_datamart]



COUNTは全列に対して適用するとNULLを数える
select COUNT(*)
from `hoge.kaggle_insta.orders`

COUNTは各列に対して適用するとNULLを数えない
select
count(days_since_prior_order)
from `hoge.kaggle_insta.orders`