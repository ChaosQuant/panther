# -*- coding: utf-8 -*-

#暂时使用自定义结构，可通过构建表方式生成

from sqlalchemy import Column, DateTime, Float, Index, VARCHAR, String, BigInteger, Integer, DATE, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata

class FactorMomentum(Base):
    __tablename__ = 'momentum'
    __table_args__ = (
        Index('id', 'trade_date', 'security_code', unique=True),
    )
    id = Column(VARCHAR(32), primary_key=True)
    security_code = Column(VARCHAR(24), primary_key=True)
    trade_date = Column(DATE, primary_key=True)
    ADX14D = Column(Float(53))
    ADXR14D = Column(Float(53))
    APBMA5D = Column(Float(53))
    ARC50D = Column(Float(53))
    BBI = Column(Float(53))
    BIAS10D = Column(Float(53))
    BIAS20D = Column(Float(53))
    BIAS5D = Column(Float(53))
    BIAS60D = Column(Float(53))
    CCI10D = Column(Float(53))
    CCI20D = Column(Float(53))
    CCI5D = Column(Float(53))
    CCI88D = Column(Float(53))
    ChgTo1MAvg = Column(Float(53))
    ChgTo1YAvg = Column(Float(53))
    ChgTo3MAvg = Column(Float(53))
    ChkOsci3D10D = Column(Float(53))
    ChkVol10D = Column(Float(53))
    DEA = Column(Float(53))
    EMA10D = Column(Float(53))
    EMA120D = Column(Float(53))
    EMA12D = Column(Float(53))
    EMA20D = Column(Float(53))
    EMA26D = Column(Float(53))
    EMA5D = Column(Float(53))
    EMA60D = Column(Float(53))
    EMV14D = Column(Float(53))
    EMV6D = Column(Float(53))
    Fiftytwoweekhigh = Column(Float(53))
    MA10Close = Column(Float(53))
    MA10D = Column(Float(53))
    MA10RegressCoeff12 = Column(Float(53))
    MA10RegressCoeff6 = Column(Float(53))
    MA120D = Column(Float(53))
    MA20D = Column(Float(53))
    MA5D = Column(Float(53))
    MA60D = Column(Float(53))
    MACD12D26D = Column(Float(53))
    MTM10D = Column(Float(53))
    PLRC12D = Column(Float(53))
    PLRC6D = Column(Float(53))
    PM10D = Column(Float(53))
    PM120D = Column(Float(53))
    PM20D = Column(Float(53))
    PM250D = Column(Float(53))
    PM5D = Column(Float(53))
    PM60D = Column(Float(53))
    PMDif5D20D = Column(Float(53))
    PMDif5D60D = Column(Float(53))
    RCI12D = Column(Float(53))
    RCI24D = Column(Float(53))
    TEMA10D = Column(Float(53))
    TEMA5D = Column(Float(53))
    TRIX10D = Column(Float(53))
    TRIX5D = Column(Float(53))
    UOS7D14D28D = Column(Float(53))
    
class FactorReversal(Base):
    __tablename__ = 'factor_reversal'
    __table_args__ = (
        Index('id', 'trade_date', 'security_code', unique=True),
    )
    id = Column(VARCHAR(32), primary_key=True)
    security_code = Column(VARCHAR(24), primary_key=True)
    trade_date = Column(DATE, primary_key=True)
    BollDown20D = Column(Float(53))
    CMO20D = Column(Float(53))
    KDJK9D = Column(Float(53))
    MFI14D = Column(Float(53))
    MFI21D = Column(Float(53))
    Mass25D = Column(Float(53))
    ROC20D = Column(Float(53))
    ROC6D = Column(Float(53))
    RSI12D = Column(Float(53))