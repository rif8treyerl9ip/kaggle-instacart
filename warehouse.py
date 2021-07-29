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

# カーネル主成分分析→3Dプロット
'''
高次元にするとメモリに乗らないのでランダムサンプリング→KPCA
'''
# ランダムサンプリング(教師アリではないが、3次元プロットするときにクラス情報が必要なのでインデックス情報を保持してラベルもとってきた)
tmp = df_train.sample(frac=0.00).index
mini = df_train.iloc[tmp]
miniy = labels[tmp]
gc.collect()

from sklearn.decomposition import KernelPCA
scikit_kpca = KernelPCA(n_components=3, kernel='rbf', gamma=15)
X_skernpca = scikit_kpca.fit_transform(mini)

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D # 3次元プロットするためのモジュール
import seaborn as sns; sns.set_style("darkgrid") # seabornでグラフをみやすく

fig = plt.figure(figsize=(10,10))
ax = Axes3D(fig)

ax.set_xlabel("X")
ax.set_ylabel("Y")
ax.set_zlabel("Z")

ax.scatter(X_skernpca[miniy == 0, 0], X_skernpca[miniy == 0, 1], X_skernpca[miniy == 0, 2], \
            color='red', marker='^', alpha=0.5)
ax.scatter(X_skernpca[miniy == 1, 0], X_skernpca[miniy == 1, 1], X_skernpca[miniy == 1, 2], \
            s = 100,color='blue', marker='o', alpha=0.5)
ax.view_init(elev=180, azim=270) # 回転の角度指定

plt.show()




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
