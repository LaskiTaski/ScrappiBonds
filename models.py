from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime
from sqlalchemy.orm import declarative_base

import os
from dotenv import load_dotenv

load_dotenv()
LOCALHOST = os.getenv("LOCALHOST")

engine = create_engine(LOCALHOST)
Base = declarative_base()


class BondData(Base):
    __tablename__ = "bonds_data"

    id = Column(Integer, primary_key=True)
    url = Column(String)
    name = Column(String)
    quoting = Column(Float)
    repayment = Column(Float)
    market = Column(Float)
    nominal = Column(Float)
    frequency = Column(Integer)
    date = Column(Date)
    days = Column(Integer)
    isin = Column(String, unique=True)
    code = Column(String)
    qualification = Column(String)
    update_time = Column(DateTime)

    def __repr__(self):
        return f"<BondData(id={self.id}, name='{self.name}', isin='{self.isin}')>"


def create_tables():
    Base.metadata.create_all(engine)
