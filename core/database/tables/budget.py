from sqlalchemy import Column, Integer, Float, Sequence, String, Date, Boolean, JSON
from core.database.tables.utils.commons import Base
from core.database.engine import engine

import random
import string

def generate_unique_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))

class Budget(Base):
    __tablename__ = 'budget'    
    id                              = Column(Integer, primary_key=True, autoincrement=True)
    store_id                        = Column(Integer)
    item_code                       = Column(String(length=10), nullable=False, default="")
    channel                         = Column(String(50), default="")
    bdate                           = Column(Date)
    budget_value                    = Column(Float)
    store_name                      = Column(String(50), default="")
    budget_cost                     = Column(Float)
    budget_qty                      = Column(Integer)