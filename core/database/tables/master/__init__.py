from sqlalchemy import Column, DateTime, Float, Integer, SmallInteger, String, Text, Sequence, Boolean, and_, inspect
from sqlalchemy.orm import sessionmaker
from core.database.tables.utils.commons import Base
from core.database.engine import engine
from datetime import datetime as dt
from typing import Protocol

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    Sequence,
    SmallInteger,
    String,
    Text,
    and_,
    inspect,
)
from sqlalchemy.orm import sessionmaker

from core.database.engine import engine
from core.database.tables.master.schemas import SupplierMasterIn
from core.database.tables.utils.commons import Base


class MasterDataModelProtocol(Protocol):
    def bulk_insert(self) -> None: ...
    def truncate_table(self) -> None: ...

class PurchaseOrder(Base):
    __tablename__ = 'purchase_orders'

    po_id             = Column(Integer(), Sequence('po_id_seq'), primary_key=True)
    sku               = Column(String(length=20), nullable=False, default="")
    po_number         = Column(String(length=12), nullable=False, default="")
    po_date           = Column(DateTime(), nullable=False, default="2019-06-23T12:00:00")
    supplier_code     = Column(String(length=20), nullable=False, default="")
    qty_purchased     = Column(Integer(), nullable=False, default=0)
    qty_received      = Column(Integer(), nullable=False, default=0)
    total_amount      = Column(Float(), nullable=False, default=0.0)
    received_qty      = Column(Float(), nullable=False, default=0.0)
    deliver_remainder = Column(Float(), nullable=False, default=0.0)

    @classmethod
    def bulk_insert(cls, data):
        """
        Insert multiple records into the database in bulk.

        :param data: List of dictionaries. Each dictionary represents a record to be inserted.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.bulk_insert_mappings(cls, data)
            session.commit()

    @classmethod
    def truncate_table(cls):
        """
        Remove all records from the table in the database. The table structure remains intact.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.query(cls).delete()
            session.commit()
    
class ItemMaster(Base):
    __tablename__ = "item_master"

    item_id                = Column(Integer(), Sequence('item_id_seq'), primary_key=True)
    primary_vendor_id      = Column(String(length=20), nullable=False, default="")
    lead_time              = Column(Integer())
    av_cost                = Column(Float(), nullable=False, default=0.0)
    new_lookup_code        = Column(String(length=40), nullable=False, default="")
    companyabc             = Column(String(length=10), nullable=False, default="")
    family_abc             = Column(String(length=10), nullable=False, default="")
    item_lookup_code       = Column(String(length=25), nullable=False, default="")
    lookup_code            = Column(String(length=25), nullable=False, default="")
    description            = Column(Text(), nullable=False, default="")
    department             = Column(String(length=25), nullable=False, default="", index=True)
    category_name          = Column(String(length=12), nullable=False, default="", index=True)
    family                 = Column(String(length=20), nullable=False, default="", index=True)
    sub_family             = Column(String(length=10), nullable=False, default="", index=True)
    supplier               = Column(String(length=20), nullable=False, default="")
    sub_category           = Column(String(length=35), nullable=False, default="", index=True)
    extended_sub_category  = Column(String(length=25), nullable=False, default="")
    sub_category_supplier  = Column(String(length=40), nullable=False, default="", index=True)
    assembly_code_nickname = Column(Text(), nullable=False, default="")
    status                 = Column(Boolean(), nullable=False, default=True)
    dom_comm               = Column(String(length=10), nullable=False, default="")
    item_type              = Column(Integer())
    company_abc            = Column(String(length=4), nullable=False, default="")
    bom_unit_id            = Column(String(length=8), nullable=False, default="")
    alt_invent_size_id     = Column(String(length=10), nullable=False, default="")
    alt_invent_color_id    = Column(String(length=10), nullable=False, default="")
    height                 = Column(Float(), nullable=False, default=0.0)
    width                  = Column(Float(), nullable=False, default=0.0)
    unit_volume            = Column(Float(), nullable=False, default=0.0)
    net_weight             = Column(Float(), nullable=False, default=0.0)

    @classmethod
    def bulk_insert(cls, data):
        """
        Insert multiple records into the database in bulk.

        :param data: List of dictionaries. Each dictionary represents a record to be inserted.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.bulk_insert_mappings(cls, data)
            session.commit()

    @classmethod
    def scan(cls, description=None, item_lookup_code=None, return_all=False, columns=None):
        """
        Scan the table based on given filter criteria and specified columns. If no criteria is provided and return_all is True,
        it returns all items.
        
        :param description: Search string for the description.
        :param item_lookup_code: Search string for the item lookup code.
        :param return_all: Whether to return all items if no criteria is provided.
        :param columns: List of column names to retrieve.
        :return: List of items matching the criteria or all items, with specified columns.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            filters = []
            
            if description:
                filters.append(cls.description.ilike(f"%{description}%"))
            if item_lookup_code:
                filters.append(cls.item_lookup_code.ilike(f"%{item_lookup_code}%"))
            
            if columns:
                # Map column names to the actual column objects
                columns_to_retrieve = [getattr(cls, col) for col in columns if hasattr(cls, col)]
            else:
                # If no specific columns are provided, retrieve all columns
                mapper = inspect(cls)
                columns_to_retrieve = [column for column in mapper.attrs]
            
            if filters:
                results = session.query(*columns_to_retrieve).filter(and_(*filters)).all()
            elif return_all:
                results = session.query(*columns_to_retrieve).all()
            else:
                results = []

            # Convert results to dictionaries if specific columns are provided
            if columns:
                results = [dict(zip(columns, row)) for row in results]

            return results
    
    @classmethod
    def count(cls):
        """
        Count the total number of records in the table.

        :return: Integer representing the total number of records.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            return session.query(cls).count()

    @classmethod
    def paginate(cls, page=1, per_page=10):
        """
        Retrieve records in a paginated manner.

        :param page: Integer representing the page number to retrieve.
        :param per_page: Integer representing the number of records per page.
        :return: List of records for the specified page.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            return session.query(cls).offset((page - 1) * per_page).limit(per_page).all()
        
    @classmethod
    def truncate_table(cls):
        """
        Remove all records from the table in the database. The table structure remains intact.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.query(cls).delete()
            session.commit()

class Stock(Base):
    __tablename__ = 'stock'
    
    item_id           = Column(Integer(), Sequence('item_stock_id_seq'), primary_key=True)
    item_lookup_code  = Column(String(30), index=True)
    item_price        = Column(Integer())
    store_id          = Column('StoreID', String(25))
    Quantity = Column('Quantity', Integer, default=0)
    QuantityCommitted = Column('QuantityCommitted', Integer, default=0)
    AvailableQTY = Column('AvailableQTY', Integer, default=0)
    Ins_OrderQTY = Column('Ins_OrderQTY', Integer, default=0)
    OutletQTY = Column('OutletQTY', Integer)
    SnapShotTime = Column('SnapShotTime', Date)
    ID = Column('ID', String(20))
    Region = Column('Region', String(20))
    OMS_NotProcessedInsOther_Orders = Column('OMS_NotProcessedInsOther_Orders', Integer, default=0)
    Transfer_out_NOT_Processed = Column('Transfer_out_NOT_Processed', Integer, default=0)
    InstOrderQTYAll = Column('InstOrderQTYAll', Integer, default=0)
    OMS_NotProcessedIns_Orders = Column('OMS_NotProcessedIns_Orders', Integer, default=0)
    ItemStatus = Column('ItemStatus', String(20), default='ACTIVE')
    storename = Column('storename', String(50))
    Description = Column('Description', String(50))
    Department = Column('Department', String(50))
    Category = Column('Category', String(50))
    Family = Column('Family', String(50))
    subfamily = Column('subfamily', String(50))
    dom_comm = Column('dom_comm', String(50))
    subcategory = Column('subcategory', String(50))
    extendedsubcategory = Column('extendedsubcategory', String(50))
    Channel = Column('Channel', String(50))
    Cost = Column('Cost', Integer())
    itemtype = Column('itemtype', String(50))
    company_abc = Column('company_abc', String(50))
    family_abc = Column('family_abc', String(50))
    lastreceivingdate = Column('lastreceivingdate', Date)

class StoreMaster(Base):
    __tablename__ = 'store_master'

    store_id              = Column(Integer(), Sequence('store_id_seq'), primary_key=True)
    name                  = Column(String(length=200), nullable=False, default="")
    capacity              = Column(Integer(), nullable=False, default=0)
    region_id             = Column(Integer(), nullable=False, default=0)
    total_cbm             = Column(Integer(), nullable=False, default=0)
    ar_name               = Column(String(length=200), nullable=False, default="")
    is_pick_up            = Column(Boolean(), nullable=False, default=False)
    is_warehouse          = Column(Boolean(), nullable=False, default=False)
    is_delivery           = Column(Boolean(), nullable=False, default=False)
    active                = Column(Boolean(), nullable=False, default=False)
    in_availability       = Column(Boolean(), nullable=False, default=False)
    is_dmda               = Column(Boolean(), nullable=False, default=False)
    is_d45                = Column(Boolean(), nullable=False, default=False)
    is_online_warehouse   = Column(Boolean(), nullable=False, default=False)
    is_norma              = Column(Boolean(), nullable=False, default=False)
    is_nps                = Column(Boolean(), nullable=False, default=False)
    is_outlet             = Column(Boolean(), nullable=False, default=False)
    exclude_delivery_fees = Column(Boolean(), nullable=False, default=False)
    dynamics_d365         = Column(String(length=100), nullable=False, default="")
    scmap                 = Column(Integer(), nullable=False, default=0)

    @classmethod
    def bulk_insert(cls, data):
        """
        Insert multiple records into the database in bulk.

        :param data: List of dictionaries. Each dictionary represents a record to be inserted.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.bulk_insert_mappings(cls, data)
            session.commit()

    @classmethod
    def truncate_table(cls):
        """
        Remove all records from the table in the database. The table structure remains intact.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.query(cls).delete()
            session.commit()

class SupplierMaster(Base):
    __tablename__ = 'supplier_master'

    supplier_id       = Column(Integer(), Sequence('supplier_id_seq'), primary_key=True)
    supplier_code     = Column(String(20), nullable=False, default="")
    vendor_account    = Column(String(20), nullable=False, default="", index=True)
    name              = Column(String(50), nullable=False, default="")
    payment_terms     = Column(String(50), nullable=False, default="")
    delivery_address  = Column(String(50), nullable=False, default="")
    supplier_location = Column(String(20), nullable=False, default="")
    supplier_contact  = Column(String(50), nullable=False, default="")
    credit_days       = Column(SmallInteger(), default=None)
    credit_limit      = Column(Integer(), default=None)
    currency          = Column(String(20), nullable=False, default="EGP")
    lead_time         = Column(SmallInteger(), default=None)
    terms             = Column(String(500), nullable=False, default="")

    @classmethod
    def bulk_insert(cls, data):
        """
        Insert multiple records into the database in bulk.

        :param data: List of dictionaries. Each dictionary represents a record to be inserted.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.bulk_insert_mappings(cls, data)
            session.commit()

    @classmethod
    def truncate_table(cls):
        """
        Remove all records from the table in the database. The table structure remains intact.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.query(cls).delete()
            session.commit()

class SalesRecords(Base):
    __tablename__ = "sales_records"

    sales_record_id   = Column(Integer(), Sequence('sales_rec_id_seq'), primary_key=True)
    source_store_name = Column(String(100), nullable=False, default="")
    order_store       = Column(String(100), nullable=False, default="")
    trx_store_id      = Column(String(15), nullable=False, default="")
    trx_store_name    = Column(String(35), nullable=False, default="")
    customer_type     = Column(String(20), nullable=False, default="")
    trx_id            = Column(String(20), nullable=False, default="")
    sold_date         = Column(Date(), index=True, nullable=False)
    item_code         = Column(String(25), index=True, nullable=False, default="")
    description       = Column(String(100), nullable=False, default="")
    department_name   = Column(String(20), nullable=False, default="")
    category_name     = Column(String(20), index=True, nullable=False, default="")
    family            = Column(String(20), index=True, nullable=False, default="")
    dom_comm          = Column(String(15), nullable=False, default="")
    supplier          = Column(String(20), index=True, nullable=False, default="")
    sub_category      = Column(String(35), index=True, nullable=False, default="")
    extd_sub_category = Column(String(25), nullable=False, default="")
    quantity          = Column(Integer(), nullable=False, default=0)
    full_discount     = Column(Float(), nullable=False, default=0.0)
    net_sales         = Column(Float(), nullable=False, default=0.0)
    sales_tax         = Column(Float(), nullable=False, default=0.0)
    gross_sales       = Column(Float(), nullable=False, default=0.0)
    total_sales_price = Column(Float(), nullable=False, default=0.0)
    full_price        = Column(Float(), nullable=False, default=0.0)
    item_cost         = Column(Float(), nullable=False, default=0.0)

    @classmethod
    def add_record(cls, **kwargs):
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            try:
                if "sold_date" not in kwargs:
                    kwargs["sold_date"] = dt.utcnow()
                new_record = cls(**kwargs)
                session.add(new_record)
                session.commit()
                return new_record.sales_record_id
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")

    @classmethod
    def bulk_insert(cls, data):
        """
        Insert multiple records into the database in bulk.

        :param data: List of dictionaries. Each dictionary represents a record to be inserted.
        """
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            session.bulk_insert_mappings(cls, data)
            session.commit()
            
    @classmethod
    def fetch_records(cls, sold_date=None, item_code=None, category_name=None, family=None, supplier=None, sub_category=None):
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        with Session() as session:
            try:
                query = session.query(cls)

                if sold_date:
                    # Check if sold_date is a tuple or list (indicating a range)
                    if isinstance(sold_date, (tuple, list)) and len(sold_date) == 2:
                        start_date, end_date = sold_date
                        query = query.filter(cls.sold_date.between(start_date, end_date))
                    else:
                        query = query.filter(cls.sold_date == sold_date)

                if item_code:
                    query = query.filter(cls.item_code == item_code)
                
                if category_name:
                    query = query.filter(cls.category_name == category_name)
                
                if family:
                    query = query.filter(cls.family == family)
                
                if supplier:
                    query = query.filter(cls.supplier == supplier)
                
                if sub_category:
                    query = query.filter(cls.sub_category == sub_category)

                return query.all()

            except Exception as e:
                print(f"Error: {e}")
                return []