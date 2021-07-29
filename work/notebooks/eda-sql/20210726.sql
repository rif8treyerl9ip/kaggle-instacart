select *
from hoge.orders
where eval_set ='train'

-- 型変換はCAST
SELECT
    CAST(product_id as STRING)
FROM
  hoge.order_products__prior

-- 型変換+文字列結合
SELECT
  order_id,
  STRING_AGG(CAST(product_id as STRING), ' ') AS products_list
FROM
  hoge.order_products__prior
GROUP BY
  order_id
-- メモ　類似メソッドにCONVERT,GROUP_CONCAT
がある。

