# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime, Float, Index, VARCHAR, String, BigInteger, Integer, DATE, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class GLInternalCode(Base):
    __tablename__ = 'gl_internal_code'
    __table_args__ = (
        Index('id', 'security_code', 'symbol', unique=True),
    )

    id = Column(VARCHAR(32), primary_key=True)
    security_code = Column(VARCHAR(24), primary_key=True)
    symbol = Column(VARCHAR(24), primary_key=True)


class Industry(Base):
    __tablename__ = 'sw_industry'
    __table_args__ = (
        Index('id', 'trade_date', 'isymbol', unique=True),
    )
    id = Column(VARCHAR(32), primary_key=True)
    isymbol = Column(VARCHAR(24))
    trade_date = Column(DATE)
    iname = Column(VARCHAR(128))
    symbol = Column(VARCHAR(32))
    sname = Column(VARCHAR(128))
    weighing = Column(DECIMAL(8, 2))


class IndexMarket(Base):
    __tablename__ = 'index_daily_price'
    __table_args__ = (
        Index('id', 'trade_date', 'security_code', unique=True),
    )
    id = Column(VARCHAR(32), primary_key=True)
    security_code = Column(VARCHAR(24), primary_key=True)
    trade_date = Column(DATE, primary_key=True)
    name = Column(VARCHAR(50))
    pre_close = Column(DECIMAL(15, 6))
    open = Column(DECIMAL(15, 6))
    close = Column(DECIMAL(15, 6))
    high = Column(DECIMAL(15, 6))
    low = Column(DECIMAL(15, 6))
    volume = Column(DECIMAL(20, 2))
    money = Column(DECIMAL(18, 3))
    deals = Column(DECIMAL(10, 0))
    change = Column(DECIMAL(9, 4))
    change_pct = Column(DECIMAL(8, 4))
    circulating_market_cap = Column(DECIMAL(18, 4))
    market_cap = Column(DECIMAL(9, 4))


class Market(Base):
    __tablename__ = 'sk_daily_price_new'
    __table_args__ = (
        Index('id', 'trade_date', 'security_code', unique=True),
    )
    id = Column(VARCHAR(32), primary_key=True)
    # symbol = Column(VARCHAR(24), primary_key=True)
    security_code = Column(VARCHAR(24), primary_key=True)
    trade_date = Column(DATE, primary_key=True)
    name = Column(VARCHAR(50))
    pre_close = Column(DECIMAL(15, 6))
    open = Column(DECIMAL(15, 6))
    close = Column(DECIMAL(15, 6))
    high = Column(DECIMAL(15, 6))
    low = Column(DECIMAL(15, 6))
    volume = Column(DECIMAL(20, 2))
    money = Column(DECIMAL(18, 3))
    deals = Column(DECIMAL(10, 0))
    change = Column(DECIMAL(9, 4))
    change_pct = Column(DECIMAL(8, 4))
    tot_mkt_cap = Column(DECIMAL(18, 4))
    turn_rate = Column(DECIMAL(9, 4))
    pre_factor = Column(DECIMAL(9, 4))
    lat_factor = Column(DECIMAL(9, 4))
    # factor = Column(DECIMAL(9, 4))
    # ltd_factor = Column(DECIMAL(9, 4))


class Exposure(Base):
    __tablename__ = 'risk_exposure'
    __table_args__ = (
        Index('trade_date', 'security_code', unique=True),
    )
    trade_date = Column(DateTime, primary_key=True, nullable=False)
    # symbol = Column(String, primary_key=True, nullable=False)
    security_code = Column(VARCHAR(24), primary_key=True)
    BETA = Column(Float(53))
    MOMENTUM = Column(Float(53))
    SIZE = Column(Float(53))
    EARNYILD = Column(Float(53))
    RESVOL = Column(Float(53))
    GROWTH = Column(Float(53))
    BTOP = Column(Float(53))
    LEVERAGE = Column(Float(53))
    LIQUIDTY = Column(Float(53))
    SIZENL = Column(Float(53))
    Bank = Column(Float(53))
    RealEstate = Column(Float(53))
    Health = Column(Float(53))
    Transportation = Column(Float(53))
    Mining = Column(Float(53))
    NonFerMetal = Column(Float(53))
    HouseApp = Column(Float(53))
    LeiService = Column(Float(53))
    MachiEquip = Column(Float(53))
    BuildDeco = Column(Float(53))
    CommeTrade = Column(Float(53))
    CONMAT = Column(Float(53))
    Auto = Column(Float(53))
    Textile = Column(Float(53))
    FoodBever = Column(Float(53))
    Electronics = Column(Float(53))
    Computer = Column(Float(53))
    LightIndus = Column(Float(53))
    Utilities = Column(Float(53))
    Telecom = Column(Float(53))
    AgriForest = Column(Float(53))
    CHEM = Column(Float(53))
    Media = Column(Float(53))
    IronSteel = Column(Float(53))
    NonBankFinan = Column(Float(53))
    ELECEQP = Column(Float(53))
    AERODEF = Column(Float(53))
    Conglomerates = Column(Float(53))
    COUNTRY = Column(Float(53))


class Index(Base):
    __tablename__ = 'index'
    __table_args__ = (
        Index('id', 'trade_date', 'isymbol', unique=True),
    )
    id = Column(VARCHAR(32), primary_key=True)
    isymbol = Column(VARCHAR(24))
    trade_date = Column(DATE)
    iname = Column(VARCHAR(128))
    symbol = Column(VARCHAR(32))
    sname = Column(VARCHAR(128))
    weighing = Column(DECIMAL(8, 2))

class Integrated(Base):
    __tablename__ = 'factor_integrated_basic'
    # __table_args__ = (
    #     Index('id', 'factor_name', unique=True),
    # )
    id = Column(VARCHAR(32))
    factor_name = Column(VARCHAR(50), primary_key=True)
    benchmark = Column(VARCHAR(20), primary_key=True)
    factor_neu = Column(Integer, primary_key=True)
    group_neu = Column(Integer, primary_key=True)
    trade_date = Column(DATE, primary_key=True)
    ret_q1 = Column(DECIMAL(26, 6))
    ret_q2 = Column(DECIMAL(26, 6))
    ret_q3 = Column(DECIMAL(26, 6))
    ret_q4 = Column(DECIMAL(26, 6))
    ret_q5 = Column(DECIMAL(26, 6))
    spread = Column(DECIMAL(26, 6))
