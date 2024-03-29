from sqlalchemy import Column, Integer, Float, Sequence, String, Date, Boolean, JSON
from core.database.tables.utils.commons import Base
from core.database.engine import engine

import random
import string

def generate_unique_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))

class ASR(Base):
    __tablename__ = 'asr'

    replenishment_id                 = Column(Integer, Sequence('asr_id_seq'), primary_key=True, autoincrement=True)
    unique_id                        = Column(String(8), unique=True, default=generate_unique_id)
    sku                              = Column(String(50), default="")
    description                      = Column(String(100), default="")
    store_code                       = Column(Integer, default=0)
    stock_on_hand_qty                = Column(Integer, default=0)
    average_sales                    = Column(Float, default=0.0)
    average_sold_qty                 = Column(Integer, default=0)
    forward_estimate_value           = Column(Float, default=0.0)
    build_to_figure_qty              = Column(Integer, default=0)
    applied_ratio                    = Column(Float, default=0.0)
    recommended_build_qty            = Column(Integer, default=0)
    percent_increase_on_build_figure = Column(Float, default=0.0)
    days_on_hand                     = Column(Integer, default=0)
    demand_value                     = Column(Float, default=0.0)
    filters                          = Column(JSON, nullable=False, default=dict)
    store_priority                   = Column(String(100), default="")
    wh_stock                         = Column(Integer, default=0)
    replenishment_qty                = Column(Integer, default=0)
    stock_required                   = Column(Integer, default=0)
    sales_channel                    = Column(String(10), default="")
    product_family                   = Column(String(20), default="")
    product_sub_family               = Column(String(20), default="")
    brand_or_supplier                = Column(String(20), default="")
    category                         = Column(String(20), default="")
    sub_category                     = Column(String(20), default="")
    status                           = Column(String(10), default="")
    po_type                          = Column(Integer, default=0)
    warehouses                       = Column(JSON, nullable=False, default=dict)
    approval_demand_value            = Column(Float, default=0.0)
    approved                         = Column(Boolean, default=False)
    pick_sheet                       = Column(JSON, nullable=False, default=dict)
    created_by                       = Column(Integer, nullable=False, default=0)
    created_on                       = Column(Date, nullable=False)
    approved_by                      = Column(Integer, default=0)
    approved_on                      = Column(Date)
    last_updated_by                  = Column(Integer, default=0)
    last_updated_on                  = Column(Date)
