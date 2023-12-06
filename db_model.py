import os, logging, yaml, asyncio, math, enum, sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Numeric, Date, LargeBinary, Enum
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from sqlalchemy_utils import ChoiceType
from datetime import date, timedelta, datetime, timezone
import uuid
import pyodbc
from sqlalchemy import create_engine
import urllib
import random

Base = declarative_base()

class TransactionInstance(Base):
      __tablename__ = 'transaction'
      timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), primary_key=True)
      account_number = Column(String)
      quantity = Column(Numeric)
      price = Column(Numeric)

if __name__ == "__main__":
    
      # Set up environment
      load_dotenv()
      logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
      level = logging.INFO
      logger = logging.getLogger()
      logger.setLevel(level)
      for handler in logger.handlers:
            handler.setLevel(level)

      # Create tables
      params = urllib.parse.quote_plus(os.getenv('AZURE_SQL_CONNECTIONSTRING'))
      conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
      engine = create_engine(conn_str, echo=True)
      db_session = sessionmaker(bind=engine)()
      Base.metadata.create_all(engine)

      # Add a "transaction"
      transaction = TransactionInstance(
            account_number = "Test account",
            quantity = random.random(),
            price = random.random() * 1000
      )     

      db_session.add(transaction)
      db_session.commit()

      # Query database
      transactions = db_session.query(TransactionInstance).order_by(TransactionInstance.timestamp).all()
      for idx, row in enumerate(transactions):
            print(f"{row.timestamp}")
