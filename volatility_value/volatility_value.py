# -*- coding: utf-8 -*-
import six,pdb,talib,pickle
import numpy as np
import pandas as pd
from utilities.singleton import Singleton
from scipy import stats

@six.add_metaclass(Singleton)
class VolatilityValue(object):
    def __init__(self):
        __str__ = 'volatility_value'
        self.name = '收益风险'
        self.factor_type1 = '收益风险'
        self.factor_type2 = '收益风险'
        self.description = '主要用于衡量收益波动性'

    def _VarianceXD(self, data, timeperiod):
        """
        X日收益方差
        :param data:
        :param timeperiod:
        :return: sigma^2 = Var(r) = E(r-E(r))^2, 因子值为年化后的值，等于日度方差*250
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        variance = np.var(returns.iloc[-timeperiod:])*250
        return variance

    def Variance20D(self, data, dependencies=['returns'], max_window=20):
        """
        :name: 20日收益方差
        :desc: 20日收益方差(20 Day of Variance), sigma^2 = Var(r) = E(r-E(r))^2, 因子值为年化后的值，等于日度方差*250
        """
        return self._VarianceXD(data, 20)

    def Variance60D(self, data, dependencies=['returns'], max_window=60):
        """
        :name: 60日收益方差
        :desc: 60日收益方差(60 Day of Variance), sigma^2 = Var(r) = E(r-E(r))^2, 因子值为年化后的值，等于日度方差*250
        """
        return self._VarianceXD(data, 60)

    def Variance120D(self, data, dependencies=['returns'], max_window=120):
        """
        :name: 120日收益方差
        :desc: 120日收益方差(120 Day of Variance), sigma^2 = Var(r) = E(r-E(r))^2, 因子值为年化后的值，等于日度方差*250
        """
        return self._VarianceXD(data, 120)

    def _KurtosisXD(self, data, timeperiod):
        """
        个股收益的X日峰度
        :param data:
        :param timeperiod:
        :return: k = E(r-E(r))^4/sigma^4
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        return pd.Series(stats.kurtosis(returns.iloc[-timeperiod:]), index=returns.columns)

    def Kurtosis20D(self, data, dependencies=['returns'], max_window=20):
        """
        :name: 个股收益的20日峰度
        :desc: 个股收益的20日峰度(20D Kurtosis), k = E(r-E(r))^4/sigma^4
        """
        return self._KurtosisXD(data, 20)

    def Kurtosis60D(self, data, dependencies=['returns'], max_window=60):
        """
        :name: 个股收益的60日峰度
        :desc: 个股收益的60日峰度(60D Kurtosis), k = E(r-E(r))^4/sigma^4
        """
        return self._KurtosisXD(data, 60)

    def Kurtosis120D(self, data, dependencies=['returns'], max_window=120):
        """
        :name: 个股收益的120日峰度
        :desc: 个股收益的120日峰度(120D Kurtosis), k = E(r-E(r))^4/sigma^4
        """
        return self._KurtosisXD(data, 120)

    def _AlphaXD(self, data, timeperiod):
        """
        X日 Jensen's Alpha
        :param data:
        :param timeperiod:
        :return: alpha = (E(r)-rf) - beta(rm-rf), r代表每日收益，rf代表无风险收益，rm代表市场收益，beta代表收益的beta值
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        returns_index = data['returns_index'].copy().fillna(method='ffill').fillna(0)

        rf = 0.03/252
        beta = self._BetaXD(data, timeperiod)

        return (returns.mean()-rf)-beta*(returns.mean()-returns_index.mean())*250

    def Alpha20D(self, data, dependencies=['returns', 'returns_index'], max_window=20):
        """
        :name: 20日 Jensen Alpha
        :desc: 20日年化 Jensen Alpha, alpha = (E(r)-rf) - beta(rm-rf), r代表每日收益，rf代表无风险收益，rm代表市场收益，beta代表收益的beta值
        """
        return self._AlphaXD(data, 20)

    def Alpha60D(self, data, dependencies=['returns', 'returns_index'], max_window=60):
        """
        :name: 60日 Jensen Alpha
        :desc: 60日年化 Jensen Alpha, alpha = (E(r)-rf) - beta(rm-rf), r代表每日收益，rf代表无风险收益，rm代表市场收益，beta代表收益的beta值
        """
        return self._AlphaXD(data, 60)

    def Alpha120D(self, data, dependencies=['returns', 'returns_index'], max_window=120):
        """
        :name: 120日 Jensen Alpha
        :desc: 120日年化 Jensen Alpha, alpha = (E(r)-rf) - beta(rm-rf), r代表每日收益，rf代表无风险收益，rm代表市场收益，beta代表收益的beta值
        """
        return self._AlphaXD(data, 120)

    def _BetaXD(self, data, timeperiod):
        """
        X日beta
        :param data:
        :param timeperiod:
        :return: beta = cov(r, rm)/var(rm)
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        returns_index = data['returns_index'].copy().fillna(method='ffill').fillna(0)

        returns = returns.iloc[-timeperiod:]
        returns_index = returns_index.iloc[-timeperiod:]

        beta = returns.apply(lambda x: x.cov(returns_index)/returns_index.var())

        return beta

    def Beta20D(self, data, dependencies=['returns', 'returns_index'], max_window=20):
        """
        :name: 20日Beta
        :desc: 20日Beta, beta = cov(r, rm)/var(rm)
        """
        return self._BetaXD(data, 20)

    def Beta60D(self, data, dependencies=['returns', 'returns_index'], max_window=60):
        """
        :name: 60日Beta
        :desc: 60日Beta, beta = cov(r, rm)/var(rm)
        """
        return self._BetaXD(data, 60)

    def Beta120D(self, data, dependencies=['returns', 'returns_index'], max_window=120):
        """
        :name: 120日Beta
        :desc: 120日Beta, beta = cov(r, rm)/var(rm)
        """
        return self._BetaXD(data, 120)

    def Beta252D(self, data, dependencies=['returns', 'returns_index'], max_window=252):
        """
        :name: 252日Beta
        :desc: 252日Beta, beta = cov(r, rm)/var(rm)
        """
        return self._BetaXD(data, 252)

    def _SharpeXD(self, data, timeperiod):
        """
        X日夏普比率，表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑。
        :param data:
        :param timeperiod:
        :return: sharpe ratio = (E(r)-rf)/sigma,
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)

        rf = 0.03
        er = returns.iloc[-timeperiod:].mean()*252
        sigma = returns.iloc[-timeperiod:].std()*np.sqrt(252)

        return (er-rf) / sigma

    def Sharpe20D(self, data, dependencies=['returns'], max_window=20):
        """
        :name: 20日夏普比率
        :desc: 20日夏普比率，表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑，sharpe ratio = (E(r)-rf)/sigma
        """
        return self._SharpeXD(data, 20)

    def Sharpe60D(self, data, dependencies=['returns'], max_window=60):
        """
        :name: 60日夏普比率
        :desc: 60日夏普比率，表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑，sharpe ratio = (E(r)-rf)/sigma
        """
        return self._SharpeXD(data, 60)

    def Sharpe120D(self, data, dependencies=['returns'], max_window=120):
        """
        :name: 120日夏普比率
        :desc: 120日夏普比率，表示每承受一单位总风险，会产生多少的超额报酬，可以同时对策略的收益与风险进行综合考虑，sharpe ratio = (E(r)-rf)/sigma
        """
        return self._SharpeXD(data, 120)

    def _TRXD(self, data, timeperiod):
        """
        X日特诺雷比率，用以衡量投资回报率
        :param data:
        :param timeperiod:
        :return: TR = (E(r)-rf)/beta,其中r代表每日收益，E(r)代表期望收益，rf代表无风险收益，beta代表收益的beta值，因子值是年化后的值，等于日度值*250
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)

        rf = 0.03/252
        er = returns.iloc[-timeperiod:].mean()
        beta = self._BetaXD(data, timeperiod)

        return (er-rf)*252/beta

    def TR20D(self, data, dependencies=['returns', 'returns_index'], max_window=20):
        """
        :name: 20日特诺雷比率
        :desc: 20日特诺雷比率(20D Treynor Ratio),用以衡量投资回报率。TR = (E(r)-rf)/beta
        """
        return self._TRXD(data, 20)

    def TR60D(self, data, dependencies=['returns', 'returns_index'], max_window=60):
        """
        :name: 60日特诺雷比率
        :desc: 60日特诺雷比率(60D Treynor Ratio),用以衡量投资回报率。TR = (E(r)-rf)/beta
        """
        return self._TRXD(data, 60)

    def TR120D(self, data, dependencies=['returns', 'returns_index'], max_window=120):
        """
        :name: 120日特诺雷比率
        :desc: 120日特诺雷比率(120D Treynor Ratio),用以衡量投资回报率。TR = (E(r)-rf)/beta
        """
        return self._TRXD(data, 120)

    def _IRXD(self, data, timeperiod):
        """
        X日信息比率
        :param data:returns
        :param timeperiod:
        :return: IR = E(r-rm)/sqrt(var(r-rm)),其中r代表每日收益，rm代表指数收益，选用沪深300指数
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        returns_index = data['returns_index'].copy().fillna(method='ffill').fillna(0)

        active_ret = returns.iloc[-timeperiod:].sub(returns_index.iloc[-timeperiod:], axis=0)

        return active_ret.mean()/active_ret.std()

    def IR20D(self, data, dependencies=['returns', 'returns_index'], max_window=20):
        """
        :name: 20日信息比率
        :desc: 20日信息比率(20D InformationRatio), IR = E(r-rm)/sqrt(var(r-rm)),其中r代表每日收益，rm代表指数收益，选用沪深300指数
        """
        return self._IRXD(data, 20)

    def IR60D(self, data, dependencies=['returns', 'returns_index'], max_window=60):
        """
        :name: 60日信息比率
        :desc: 60日信息比率(60D InformationRatio), IR = E(r-rm)/sqrt(var(r-rm)),其中r代表每日收益，rm代表指数收益，选用沪深300指数
        """
        return self._IRXD(data, 60)

    def IR120D(self, data, dependencies=['returns', 'returns_index'], max_window=120):
        """
        :name: 120日信息比率
        :desc: 120日信息比率(120D InformationRatio), IR = E(r-rm)/sqrt(var(r-rm)),其中r代表每日收益，rm代表指数收益，选用沪深300指数
        """
        return self._IRXD(data, 120)

    def _GainVarianceXD(self, data, timeperiod):
        """
        X日收益方差，类似于方差，主要衡量收益的表现
        :param data:
        :param timeperiod:
        :return: GV(r) = E(r-E(r)|r>=0)^2 = E(r^2|r>=0) - E(r|r>=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        returns = returns.iloc[-timeperiod:] * 250

        gv = returns.apply(lambda x: x[x>=0].var())

        return gv

    def GainVariance20D(self, data, dependencies=['returns'], max_window=20):
        """
        :name: 20日收益方差
        :desc: 20日收益方差(20D Gain Variance), 类似于方差，主要衡量收益的表现. GV(r) = E(r-E(r)|r>=0)^2 = E(r^2|r>=0) - E(r|r>=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250
        """
        return self._GainVarianceXD(data, 20)

    def GainVariance60D(self, data, dependencies=['returns'], max_window=60):
        """
        :name: 60日收益方差
        :desc: 60日收益方差(60D Gain Variance), 类似于方差，主要衡量收益的表现. GV(r) = E(r-E(r)|r>=0)^2 = E(r^2|r>=0) - E(r|r>=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250
        """
        return self._GainVarianceXD(data, 60)

    def GainVariance120D(self, data, dependencies=['returns'], max_window=120):
        """
        :name: 120日收益方差
        :desc: 120日收益方差(120D Gain Variance), 类似于方差，主要衡量收益的表现. GV(r) = E(r-E(r)|r>=0)^2 = E(r^2|r>=0) - E(r|r>=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250
        """
        return self._GainVarianceXD(data, 120)

    def _LossVarianceXD(self, data, timeperiod):
        """
        X日损失方差，类似于方差，主要衡量损失的表现
        :param data:
        :param timeperiod:
        :return: LV(r) = E(r-E(r)|r<=0)^2 = E(r^2|r<=0) - E(r|r<=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)
        returns = returns.iloc[-timeperiod:] * 250

        lv = returns.apply(lambda x: x[x <= 0].var())

        return lv

    def LossVariance20D(self, data, dependencies=['returns'], max_window=20):
        """
        :name: 20日损失方差
        :desc: 20日损失方差(20D Loss Variance), 类似于方差，主要衡量损失的表现.LV(r) = E(r-E(r)|r<=0)^2 = E(r^2|r<=0) - E(r|r<=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250.
        """
        return self._LossVarianceXD(data, 20)

    def LossVariance60D(self, data, dependencies=['returns'], max_window=60):
        """
        :name: 60日损失方差
        :desc: 60日损失方差(60D Loss Variance), 类似于方差，主要衡量损失的表现.LV(r) = E(r-E(r)|r<=0)^2 = E(r^2|r<=0) - E(r|r<=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250.
        """
        return self._LossVarianceXD(data, 60)

    def LossVariance120D(self, data, dependencies=['returns'], max_window=120):
        """
        :name: 120日损失方差
        :desc: 120日损失方差(120D Loss Variance), 类似于方差，主要衡量损失的表现.LV(r) = E(r-E(r)|r<=0)^2 = E(r^2|r<=0) - E(r|r<=0)^2,其中r代表每日收益，因子值是年化后的值，等于日度值*250.
        """
        return self._LossVarianceXD(data, 120)

    def _GainLossVarianceRatioXD(self, data, timeperiod):
        """
        20 日收益损失方差比
        :param data:
        :param timeperiod:
        :return: GL Ratio = GV/LV = E(r-E(r)|r>=0)^2) / E(r-E(r)|r<=0)^2,其中r代表每日收益
        """
        GV = self._GainVarianceXD(data, timeperiod)
        LV = self._LossVarianceXD(data, timeperiod)

        return GV/LV

    def GainLossVarianceRatio20D(self, data, dependencies=['returns'], max_window=20):
        """
        :name: 20日收益损失方差比
        :desc: 20日收益损失方差比, GL Ratio = GV/LV = E(r-E(r)|r>=0)^2) / E(r-E(r)|r<=0)^2,其中r代表每日收益
        """
        return self._GainLossVarianceRatioXD(data, 20)

    def GainLossVarianceRatio60D(self, data, dependencies=['returns'], max_window=60):
        """
        :name: 70日收益损失方差比
        :desc: 70日收益损失方差比, GL Ratio = GV/LV = E(r-E(r)|r>=0)^2) / E(r-E(r)|r<=0)^2,其中r代表每日收益
        """
        return self._GainLossVarianceRatioXD(data, 70)

    def GainLossVarianceRatio120D(self, data, dependencies=['returns'], max_window=120):
        """
        :name: 120日收益损失方差比
        :desc: 120日收益损失方差比, GL Ratio = GV/LV = E(r-E(r)|r>=0)^2) / E(r-E(r)|r<=0)^2,其中r代表每日收益
        """
        return self._GainLossVarianceRatioXD(data, 120)

    def DailyReturnSTD252D(self, data, dependencies=['returns'], max_window=252):
        """
        :name: 252日超额收益标准差
        :desc: 252日超额收益标准差，DailyReturnSTD252D
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0)

        rf = 0.03/252
        return (returns.iloc[-252:]-rf).std()

    def DDNSR12M(self, data, dependencies=['returns', 'returns_index'], max_window=252):
        """
        :name:过往12个月下跌波动
        :desc:过往12个月下跌波动(12M Downside standard deviations ratio),过往12个月中，市场组合日收益为负时，日股日收益标准差和市场组合日收益标准差之比。DDNCR=sd(r)/sd(rm),其中市场组合日收益rm的计算采用沪深300的数据，仅考虑市场收益为负的数据。
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0).iloc[-252:]
        returns_index = data['returns_index'].copy().fillna(method='ffill').fillna(0).iloc[-252:]

        returns = returns[returns_index <= 0]
        returns_index = returns_index[returns_index <= 0]

        return returns.std()/returns_index.std()

    def DDNCR12M(self, data, dependencies=['returns', 'returns_index'], max_window=252):
        """
        :name: 下跌相关系数
        :desc:
        """
        returns = data['returns'].copy().fillna(method='ffill').fillna(0).iloc[-252:]
        returns_index = data['returns_index'].copy().fillna(method='ffill').fillna(0).iloc[-252:]

        returns = returns[returns_index <= 0]
        returns_index = returns_index[returns_index <= 0]

        return returns.apply(lambda x: x.corr(returns_index))

    # def DVRAT(self, data, dependencies=['returns'], max_window=520):
    #     """
    #     :name: 收益相对波动
    #     :desc: 收益相对波动, 记rt股票日收益，rft为每日的无风险收益，则股票当日超额收益为et,收益相对波动表示为
    #             DVRAT = sigma_q^2/sigma^2 -1
    #     """
    #     q = 10
    #     t = 252*2
    #     m = q * (t - q + 1) * (1 - q / t)
    #
    #     returns = data['returns'].copy().fillna(method='ffill').fillna(0).iloc[-t:]
    #     returns_moving_sum = returns.rolling(window=q).sum().iloc[-(t-q+1):]
    #     returns = returns.iloc[-t:]
    #
    #     sigma_q_sq = returns_moving_sum.pow(2).fillna(0).sum()/m
    #     sigma_sq = returns.var()
    #
    #     return sigma_q_sq/sigma_sq - 1


if __name__ == "__main__":
    with open('../mkt_df.pkl', 'rb') as f:
        data = pickle.load(f)

    process = VolatilityValue()

    print("-----------Variance20D-----------")
    print(process.Variance20D(data))

    # print("-----------Kurtosis20D-----------")
    # print(process.Kurtosis20D(data))
    #
    # print("-----------Alpha20D-----------")
    # print(process.Alpha120D(data))
    #
    # print("-----------BetaXD-----------")
    # print(process.Beta252D(data))
    #
    # print("-----------SharpeXD-----------")
    # print(process.Sharpe120D(data))
    #
    # print("-----------TRXD-----------")
    # print(process.TR120D(data))
    #
    # print("-----------IRXD-----------")
    # print(process.IR120D(data))
    #
    # print("-----------GainVarianceXD-----------")
    # print(process.GainVariance120D(data))
    #
    # print("-----------LossVarianceXD-----------")
    # print(process.LossVariance60D(data))
    #
    # print("-----------GainLossVarianceRatioXD-----------")
    # print(process.GainLossVarianceRatio60D(data))
    #
    # print("-----------DDNSR12M-----------")
    # print(process.DDNSR12M(data))
    #
    # print("-----------DDNCR12M-----------")
    # print(process.DDNCR12M(data))
    #
    # print("-----------DVRAT-----------")
    # print(process.DVRAT(data))






