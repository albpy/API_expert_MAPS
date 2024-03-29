from sqlalchemy import Column, Integer, Float, Sequence, String, Date, Boolean, JSON, Text
from core.database.tables.utils.commons import Base
from core.database.engine import engine

import random
import string

class BudgetItem(Base):
    __tablename__ = 'budget_item_view'    
    item_id                = Column(Integer, Sequence('item_id_seq'), primary_key=True, autoincrement=False)
    primary_vendor_id      = Column(String(length=20), nullable=False, default="")
    lead_time              = Column(Integer)
    av_cost                = Column(Float, nullable=False, default=0.0)
    new_lookup_code        = Column(String(length=40), nullable=False, default="")
    companyabc             = Column(String(length=10), nullable=False, default="")
    family_abc             = Column(String(length=10), nullable=False, default="")
    item_lookup_code       = Column(String(length=25), nullable=False, default="")
    lookup_code            = Column(String(length=25), nullable=False, default="")
    description            = Column(Text, nullable=False, default="")
    department             = Column(String(length=25), nullable=False, default="", index=True)
    category_name          = Column(String(length=12), nullable=False, default="", index=True)
    family                 = Column(String(length=20), nullable=False, default="", index=True)
    sub_family             = Column(String(length=10), nullable=False, default="", index=True)
    supplier               = Column(String(length=20), nullable=False, default="")
    sub_category           = Column(String(length=35), nullable=False, default="", index=True)
    extended_sub_category  = Column(String(length=25), nullable=False, default="")
    sub_category_supplier  = Column(String(length=40), nullable=False, default="", index=True)
    assembly_code_nickname = Column(Text, nullable=False, default="")
    status                 = Column(Boolean, nullable=False, default=True)
    dom_comm               = Column(String(length=10), nullable=False, default="")
    item_type              = Column(Integer)
    company_abc            = Column(String(length=4), nullable=False, default="")
    bom_unit_id            = Column(String(length=8), nullable=False, default="")
    alt_invent_size_id     = Column(String(length=10), nullable=False, default="")
    alt_invent_color_id    = Column(String(length=10), nullable=False, default="")
    height                 = Column(Float, nullable=False, default=0.0)
    width                  = Column(Float, nullable=False, default=0.0)
    unit_volume            = Column(Float, nullable=False, default=0.0)
    net_weight             = Column(Float, nullable=False, default=0.0)
    store_id               = Column(Integer)
    channel                = Column(String(50), default="")
    bdate                  = Column(Date)
    budget_value           = Column(Float)
    store_name             = Column(String(50), default="")
    budget_cost            = Column(Float)
    budget_qty             = Column(Integer)