# 初めて使用した実装/普段使う実装で今後使いそうなやつをメモ

# いつもの
import os
print(os.listdir('../../'))
pd.set_option('display.max_columns',10000); pd.set_option('display.max_rows', 50); np.set_printoptions(threshold=90000)


# リスト内の整数が6倍されるのではなく、6回繰り返される、Numpyのブロードキャストと混同していた
a = [2]
a*6
[2, 2, 2, 2, 2, 2]
# 恐らく下記は同じ実装
    df['user_id'] = df.order_id.map(orders.user_id).astype(np.int32)
    df.merge(orders['user_id'], on=inner, how='order_id')と同じことが上でできてるっぽい。


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

# pickleにしたのでcsvを置いておく
priors = pd.read_csv(IDIR + 'order_products__prior.csv')
train = pd.read_csv(IDIR + 'order_products__train.csv')
orders = pd.read_csv(IDIR + 'orders.csv')
products = pd.read_csv(IDIR + 'products.csv')

print('priors {}: {}'.format(priors.shape, ', '.join(priors.columns)))
print('orders {}: {}'.format(orders.shape, ', '.join(orders.columns)))
print('train {}:    {}'.format(train.shape, ', '.join(train.columns)))
