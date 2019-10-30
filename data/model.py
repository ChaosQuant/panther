#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: model.py
@time: 2019-08-28 16:17
"""
from sqlalchemy import Column
from sqlalchemy.types import DECIMAL, DATE, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, String, Text, Boolean, text, JSON

Base = declarative_base()  # 生成ORM基类
metadata = Base.metadata


class BalanceMRQ(Base):
    __tablename__ = 'balance_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(BigInteger)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ACCORECE = Column(Float(53))
    ADVAPAYM = Column(Float(53))
    BDSPAYA = Column(Float(53))
    CONSPROG = Column(Float(53))
    CURFDS = Column(Float(53))
    DEFETAXASSET = Column(Float(53))
    # DEVEEXPE = Column(Float(53))
    ENGIMATE = Column(Float(53))
    FIXEDASSECLEATOT = Column(Float(53))
    FIXEDASSENET = Column(Float(53))
    GOODWILL = Column(Float(53))
    INTAASSET = Column(Float(53))
    INTEPAYA = Column(Float(53))
    INVE = Column(Float(53))
    LOGPREPEXPE = Column(Float(53))
    LONGBORR = Column(Float(53))
    NOTESRECE = Column(Float(53))
    OTHERRECE = Column(Float(53))
    PARESHARRIGH = Column(Float(53))
    RIGHAGGR = Column(Float(53))
    SHORTTERMBORR = Column(Float(53))
    TOTALCURRLIAB = Column(Float(53))
    TOTALNONCASSETS = Column(Float(53))
    TOTALNONCLIAB = Column(Float(53))
    TOTASSET = Column(Float(53))
    TOTCURRASSET = Column(Float(53))
    TOTLIAB = Column(Float(53))
    TRADFINASSET = Column(Float(53))
    CAPISURP = Column(Float(53))
    RESE = Column(Float(53))
    UNDIPROF = Column(Float(53))
    DUENONCLIAB = Column(Float(53))
    PREP = Column(Float(53))
    DERILIAB = Column(Float(53))
    ACCOPAYA = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class BalanceTTM(Base):
    __tablename__ = 'balance_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(BigInteger)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ACCORECE = Column(Float(53))
    ADVAPAYM = Column(Float(53))
    BDSPAYA = Column(Float(53))
    CONSPROG = Column(Float(53))
    CURFDS = Column(Float(53))
    DEFETAXASSET = Column(Float(53))
    # DEVEEXPE = Column(Float(53))
    ENGIMATE = Column(Float(53))
    FIXEDASSECLEATOT = Column(Float(53))
    FIXEDASSENET = Column(Float(53))
    GOODWILL = Column(Float(53))
    INTAASSET = Column(Float(53))
    INTEPAYA = Column(Float(53))
    INVE = Column(Float(53))
    LOGPREPEXPE = Column(Float(53))
    LONGBORR = Column(Float(53))
    NOTESRECE = Column(Float(53))
    OTHERRECE = Column(Float(53))
    PARESHARRIGH = Column(Float(53))
    RIGHAGGR = Column(Float(53))
    SHORTTERMBORR = Column(Float(53))
    TOTALCURRLIAB = Column(Float(53))
    TOTALNONCASSETS = Column(Float(53))
    TOTALNONCLIAB = Column(Float(53))
    TOTASSET = Column(Float(53))
    TOTCURRASSET = Column(Float(53))
    TOTLIAB = Column(Float(53))
    TRADFINASSET = Column(Float(53))
    CAPISURP = Column(Float(53))
    RESE = Column(Float(53))
    UNDIPROF = Column(Float(53))
    DUENONCLIAB = Column(Float(53))
    PREP = Column(Float(53))
    DERILIAB = Column(Float(53))
    ACCOPAYA = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class BalanceReport(Base):
    __tablename__ = 'balance_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ACCORECE = Column(Float(53))
    ADVAPAYM = Column(Float(53))
    BDSPAYA = Column(Float(53))
    CONSPROG = Column(Float(53))
    CURFDS = Column(Float(53))
    DEFETAXASSET = Column(Float(53))
    # DEVEEXPE = Column(Float(53))
    ENGIMATE = Column(Float(53))
    FIXEDASSECLEATOT = Column(Float(53))
    FIXEDASSENET = Column(Float(53))
    GOODWILL = Column(Float(53))
    INTAASSET = Column(Float(53))
    INTEPAYA = Column(Float(53))
    INVE = Column(Float(53))
    LOGPREPEXPE = Column(Float(53))
    LONGBORR = Column(Float(53))
    NOTESRECE = Column(Float(53))
    OTHERRECE = Column(Float(53))
    PARESHARRIGH = Column(Float(53))
    RIGHAGGR = Column(Float(53))
    SHORTTERMBORR = Column(Float(53))
    TOTALCURRLIAB = Column(Float(53))
    TOTALNONCASSETS = Column(Float(53))
    TOTALNONCLIAB = Column(Float(53))
    TOTASSET = Column(Float(53))
    TOTCURRASSET = Column(Float(53))
    TOTLIAB = Column(Float(53))
    TRADFINASSET = Column(Float(53))
    CAPISURP = Column(Float(53))
    RESE = Column(Float(53))
    UNDIPROF = Column(Float(53))
    DUENONCLIAB = Column(Float(53))
    PREP = Column(Float(53))
    DERILIAB = Column(Float(53))
    ACCOPAYA = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class CashFlowMRQ(Base):
    __tablename__ = 'cash_flow_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    MANANETR = Column(Float(53))
    INVNETCASHFLOW = Column(Float(53))
    FINNETCFLOW = Column(Float(53))
    CASHNETR = Column(Float(53))
    FINALCASHBALA = Column(Float(53))
    BIZNETCFLOW = Column(Float(53))
    CASHNETI = Column(Float(53))
    LABORGETCASH = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    INCOTAXEXPE = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class CashFlowTTM(Base):
    __tablename__ = 'cash_flow_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    MANANETR = Column(Float(53))
    INVNETCASHFLOW = Column(Float(53))
    FINNETCFLOW = Column(Float(53))
    CASHNETR = Column(Float(53))
    FINALCASHBALA = Column(Float(53))
    BIZNETCFLOW = Column(Float(53))
    CASHNETI = Column(Float(53))
    LABORGETCASH = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    INCOTAXEXPE = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class CashFlowReport(Base):
    __tablename__ = 'cash_flow_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    MANANETR = Column(DECIMAL(19, 4))
    INVNETCASHFLOW = Column(DECIMAL(19, 4))
    FINNETCFLOW = Column(DECIMAL(19, 4))
    CASHNETR = Column(DECIMAL(19, 4))
    FINALCASHBALA = Column(DECIMAL(19, 4))
    BIZNETCFLOW = Column(DECIMAL(19, 4))
    CASHNETI = Column(DECIMAL(19, 4))
    LABORGETCASH = Column(DECIMAL(19, 4))
    MINYSHARRIGH = Column(Float(53))
    INCOTAXEXPE = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IncomeTTM(Base):
    __tablename__ = 'income_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    BIZTOTINCO = Column(Float(53))
    # INTEINCO = Column(Float(53))
    BIZTOTCOST = Column(Float(53))
    BIZCOST = Column(Float(53))
    # INTEEXPE = Column(Float(53))
    # DEVEEXPE = Column(Float(53))
    SALESEXPE = Column(Float(53))
    ASSEIMPALOSS = Column(Float(53))
    # ASSOINVEPROF = Column(Float(53))
    PERPROFIT = Column(Float(53))
    TOTPROFIT = Column(Float(53))
    NETPROFIT = Column(Float(53))
    PARENETP = Column(Float(53))
    DILUTEDEPS = Column(Float(53))
    MANAEXPE = Column(Float(53))
    FINEXPE = Column(Float(53))
    BIZINCO = Column(Float(53))
    NONOREVE = Column(Float(53))
    NONOEXPE = Column(Float(53))
    BIZTAX = Column(Float(53))
    COMDIVPAYBABLE = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    INCOTAXEXPE = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IncomeMRQ(Base):
    __tablename__ = 'income_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    BIZTOTINCO = Column(Float(53))
    # INTEINCO = Column(Float(53))
    BIZTOTCOST = Column(Float(53))
    BIZCOST = Column(Float(53))
    # INTEEXPE = Column(Float(53))
    # DEVEEXPE = Column(Float(53))
    SALESEXPE = Column(Float(53))
    ASSEIMPALOSS = Column(Float(53))
    # ASSOINVEPROF = Column(Float(53))
    PERPROFIT = Column(Float(53))
    TOTPROFIT = Column(Float(53))
    NETPROFIT = Column(Float(53))
    PARENETP = Column(Float(53))
    DILUTEDEPS = Column(Float(53))
    MANAEXPE = Column(Float(53))
    FINEXPE = Column(Float(53))
    BIZINCO = Column(Float(53))
    NONOREVE = Column(Float(53))
    NONOEXPE = Column(Float(53))
    BIZTAX = Column(Float(53))
    COMDIVPAYBABLE = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    INCOTAXEXPE = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IncomeReport(Base):
    __tablename__ = 'income_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    BIZTOTINCO = Column(Float(53))
    # INTEINCO = Column(Float(53))
    BIZTOTCOST = Column(Float(53))
    BIZCOST = Column(Float(53))
    # INTEEXPE = Column(Float(53))
    # DEVEEXPE = Column(Float(53))
    SALESEXPE = Column(Float(53))
    ASSEIMPALOSS = Column(Float(53))
    # ASSOINVEPROF = Column(Float(53))
    PERPROFIT = Column(Float(53))
    TOTPROFIT = Column(Float(53))
    NETPROFIT = Column(Float(53))
    PARENETP = Column(Float(53))
    DILUTEDEPS = Column(Float(53))
    MANAEXPE = Column(Float(53))
    FINEXPE = Column(Float(53))
    BIZINCO = Column(Float(53))
    NONOREVE = Column(Float(53))
    NONOEXPE = Column(Float(53))
    BIZTAX = Column(Float(53))
    COMDIVPAYBABLE = Column(Float(53))
    MINYSHARRIGH = Column(Float(53))
    INCOTAXEXPE = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IndicatorTTM(Base):
    __tablename__ = 'indicator_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ENDPUBLISHDATE = Column(DATE)
    NPCUT = Column(Float(53))
    # EPSBASIC = Column(Float(53))
    ROE = Column(Float(53))
    ROEAVG = Column(Float(53))
    ROEDILUTED = Column(Float(53))
    ROEDILUTEDCUT = Column(Float(53))
    # ROEWEIGHTED = Column(Float(53))
    # ROEWEIGHTEDCUT = Column(Float(53))
    FCFF = Column(Float(53))
    TOTIC = Column(Float(53))
    CURRENTRT = Column(Float(53))
    QUICKRT = Column(Float(53))
    ASSLIABRT = Column(Float(53))
    EQURT = Column(Float(53))
    NDEBT = Column(Float(53))
    WORKCAP = Column(Float(53))
    TDEBTTOFART = Column(Float(53))
    ACCRECGTURNDAYS = Column(Float(53))
    INVTURNDAYS = Column(Float(53))
    # DIVCOVER = Column(Float(53))
    ACCPAYTDAYS = Column(Float(53))
    NNONOPITOTP = Column(Float(53))
    NONINTNONCURLIAB = Column(Float(53))
    NONINTCURLIABS = Column(Float(53))
    SCASHREVTOOPIRT = Column(Float(53))
    OPANITOTP = Column(Float(53))
    RETAINEDEAR = Column(Float(53))
    FCFE = Column(Float(53))
    NOPI = Column(Float(53))
    # CURDEPANDAMOR = Column(Float(53))
    NPTOAVGTA = Column(Float(53))
    SCOSTRT = Column(Float(53))
    SGPMARGIN = Column(Float(53))
    ROA = Column(Float(53))
    ROIC = Column(Float(53))
    OPEXPRT = Column(Float(53))
    MGTEXPRT = Column(Float(53))
    FINLEXPRT = Column(Float(53))
    OPPRORT = Column(Float(53))
    OPNCFTOTLIAB = Column(Float(53))
    OPGPMARGIN = Column(Float(53))
    EBITTOTOPI = Column(Float(53))
    OPPTOTP = Column(Float(53))
    OPNCFTONDABT = Column(Float(53))
    NVALCHGITOTP = Column(Float(53))
    # DPS = Column(Float(53))
    NEGAL = Column(Float(53))
    NETPROFITCUT = Column(Float(53))
    NVALCHGIT = Column(Float(53))
    EBITDA = Column(Float(53))
    EBITFORP = Column(Float(53))
    EBIT = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IndicatorMRQ(Base):
    __tablename__ = 'indicator_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ENDPUBLISHDATE = Column(DATE)
    NPCUT = Column(Float(53))
    # EPSBASIC = Column(Float(53))
    # ROE = Column(Float(53))
    ROEAVG = Column(Float(53))
    ROEDILUTED = Column(Float(53))
    ROEDILUTEDCUT = Column(Float(53))
    # ROEWEIGHTED = Column(Float(53))
    # ROEWEIGHTEDCUT = Column(Float(53))
    FCFF = Column(Float(53))
    TOTIC = Column(Float(53))
    CURRENTRT = Column(Float(53))
    QUICKRT = Column(Float(53))
    ASSLIABRT = Column(Float(53))
    EQURT = Column(Float(53))
    NDEBT = Column(Float(53))
    WORKCAP = Column(Float(53))
    TDEBTTOFART = Column(Float(53))
    ACCRECGTURNDAYS = Column(Float(53))
    INVTURNDAYS = Column(Float(53))
    # # DIVCOVER = Column(Float(53))
    ACCPAYTDAYS = Column(Float(53))
    NNONOPITOTP = Column(Float(53))
    NONINTNONCURLIAB = Column(Float(53))
    NONINTCURLIABS = Column(Float(53))
    SCASHREVTOOPIRT = Column(Float(53))
    OPANITOTP = Column(Float(53))
    RETAINEDEAR = Column(Float(53))
    FCFE = Column(Float(53))
    NOPI = Column(Float(53))
    # CURDEPANDAMOR = Column(Float(53))
    NPTOAVGTA = Column(Float(53))
    SCOSTRT = Column(Float(53))
    SGPMARGIN = Column(Float(53))
    ROA = Column(Float(53))
    ROIC = Column(Float(53))
    OPEXPRT = Column(Float(53))
    MGTEXPRT = Column(Float(53))
    FINLEXPRT = Column(Float(53))
    OPPRORT = Column(Float(53))
    OPNCFTOTLIAB = Column(Float(53))
    OPGPMARGIN = Column(Float(53))
    EBITTOTOPI = Column(Float(53))
    OPPTOTP = Column(Float(53))
    OPNCFTONDABT = Column(Float(53))
    NVALCHGITOTP = Column(Float(53))
    # DPS = Column(Float(53))
    NEGAL = Column(Float(53))
    NETPROFITCUT = Column(Float(53))
    NVALCHGIT = Column(Float(53))
    EBITDA = Column(Float(53))
    EBITFORP = Column(Float(53))
    EBIT = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IndicatorReport(Base):
    __tablename__ = 'indicator_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ENDPUBLISHDATE = Column(DATE)
    NPCUT = Column(Float(53))
    EPSBASIC = Column(Float(53))
    # ROE = Column(Float(53))
    ROEAVG = Column(Float(53))
    # ROEDILUTED = Column(Float(53))
    # ROEDILUTEDCUT = Column(Float(53))
    ROEWEIGHTED = Column(Float(53))
    ROEWEIGHTEDCUT = Column(Float(53))
    FCFF = Column(Float(53))
    TOTIC = Column(Float(53))
    CURRENTRT = Column(Float(53))
    QUICKRT = Column(Float(53))
    ASSLIABRT = Column(Float(53))
    EQURT = Column(Float(53))
    NDEBT = Column(Float(53))
    WORKCAP = Column(Float(53))
    TDEBTTOFART = Column(Float(53))
    ACCRECGTURNDAYS = Column(Float(53))
    INVTURNDAYS = Column(Float(53))
    # DIVCOVER = Column(Float(53))
    ACCPAYTDAYS = Column(Float(53))
    NNONOPITOTP = Column(Float(53))
    NONINTNONCURLIAB = Column(Float(53))
    NONINTCURLIABS = Column(Float(53))
    SCASHREVTOOPIRT = Column(Float(53))
    OPANITOTP = Column(Float(53))
    RETAINEDEAR = Column(Float(53))
    FCFE = Column(Float(53))
    NOPI = Column(Float(53))
    # CURDEPANDAMOR = Column(Float(53))
    NPTOAVGTA = Column(Float(53))
    SCOSTRT = Column(Float(53))
    SGPMARGIN = Column(Float(53))
    ROA = Column(Float(53))
    ROIC = Column(Float(53))
    OPEXPRT = Column(Float(53))
    MGTEXPRT = Column(Float(53))
    FINLEXPRT = Column(Float(53))
    OPPRORT = Column(Float(53))
    OPNCFTOTLIAB = Column(Float(53))
    OPGPMARGIN = Column(Float(53))
    EBITTOTOPI = Column(Float(53))
    OPPTOTP = Column(Float(53))
    OPNCFTONDABT = Column(Float(53))
    NVALCHGITOTP = Column(Float(53))
    # DPS = Column(Float(53))
    NEGAL = Column(Float(53))
    NETPROFITCUT = Column(Float(53))
    NVALCHGIT = Column(Float(53))
    EBITDA = Column(Float(53))
    EBITFORP = Column(Float(53))
    EBIT = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }
