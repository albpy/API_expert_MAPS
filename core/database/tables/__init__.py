from core.database import engine

from .admin import Policy, Role, Services, User
from .asr import ASR
<<<<<<< HEAD
from .master import (
    ItemMaster,
    MasterDataModelProtocol,
    PurchaseOrder,
    SalesRecords,
    StoreMaster,
    SupplierMaster,
)
from .utils.commons import Base
=======
from .admin import User, Role, Policy, Services
from .master import PurchaseOrder, ItemMaster, StoreMaster, SupplierMaster
>>>>>>> main

# Create the database tables
Base.metadata.create_all(bind=engine)

target_metadata = Base.metadata

__all__ = [
    "ASR",
    "User", "Role", "Policy", "Services",
<<<<<<< HEAD
    "PurchaseOrder", "ItemMaster", "StoreMaster", "SupplierMaster", "SalesRecords", "MasterDataModelProtocol",
=======
    "PurchaseOrder", "ItemMaster", "StoreMaster", "SupplierMaster",
>>>>>>> main
    "target_metadata"
]