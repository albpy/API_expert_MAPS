from sqlalchemy import Column, Integer, Float, Sequence, String, Date, Boolean, JSON
from core.database.tables.utils.commons import Base
from core.database.engine import engine

import random
import string

class Stock(Base):
    __tablename__ = 'stock'
    index                             = Column(Integer, Sequence('stock_id'), primary_key=True, autoincrement=True)
    table_id                          = Column(Integer)
    item_lookup_code                  = Column(String(24), default="")
    name                              = Column(String(30), default="")
    price                             = Column(Float, default=0.0)
    item_name                         = Column(String(35), default="")
    store_id                          = Column(Integer)
    quantiy                           = Column(Integer)
    quantiy_commited                  = Column(Float, default=0.0)
    available_quantity                = Column(Integer)
    ins_order_quantity                = Column(Integer)
    outlet_quantity                   = Column(Integer)
    snap_shot_time                    = Column(String(10), default="")
    id                                = Column(String(10), default="")
    region                            = Column(String(10), default="")
    oms_not_processins_other_orders   = Column(Integer)
    transfer_out_not_processed        = Column(Integer)
    inst_order_qty_all                = Column(Integer)
    oms_not_processins_other_orders   = Column(Integer)
    item_status                       = Column(String(10), default="")
