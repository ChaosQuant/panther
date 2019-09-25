import numpy as np
import pandas as pd

def se_winsorize(se, method='sigma', limits=(3.0, 3.0), drop=False):
    """
    计算去极值序列
    :param se:
    :param method:
    :param limits:
    :param drop:
    :return:
    """
    se = se.copy(deep=True)
    if method == 'quantile':
        down, up = se.quantile([limits[0], 1.0 - limits[1]])
    elif method == 'sigma':
        std, mean = se.std(), se.mean()
        down, up = mean - limits[0]*std, mean + limits[1]*std
    elif method == 'med':
        me = se.median()
        mad = (np.abs(se - me)).median()
        down, up = me - limits[0] * 1.4826 * mad, me + limits[1] * 1.4826 * mad

    if drop:
        se[se<down] = np.NaN
        se[se>up] = np.NaN
    else:
        se[se<down] = down
        se[se>up] = up
    return se


def se_standardize(se):
    """
    计算标准化序列
    :param se:
    :return:
    """
    try:
        res = (se - se.mean()) / se.std()
    except:
        res = pd.Series(data=np.NaN, index=se.index)
    return res


def se_neutralize(se, neu):
    """
    计算中性化序列
    :param se:
    :param neu:
    :return:
    """
    try:
        se = se.dropna()
        if se.empty:
            return se
        idx = se.index.intersection(neu.index)
        se = se.loc[idx]
        neu = neu.loc[idx,:]

        x = np.linalg.lstsq(neu.values, se.values.T)[0]
        y = se - neu.dot(x)
    except:
        print('neutralize error!')
        return None
    return y
