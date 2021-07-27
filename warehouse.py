# 初めて使用した実装で今後使いそうなやつをメモ

# ユーザ事に一番新しいの注文番号だけを取得
max_order = all_df.groupby(["user_id"]).agg({'order_number': 'max'})
max_order = max_order.rename(columns={'order_number': 'max_order_number'})

all_df = all_df.merge(max_order, how='inner', on='user_id')
all_df.loc[all_df['order_number'] == all_df['max_order_number']].head(50)

# 積集合
a = set(trainn.user_id.sort_values().values)
b = set(test_orders.user_id.sort_values().values)
s_intersection = a.intersection(b)
print(s_intersection)