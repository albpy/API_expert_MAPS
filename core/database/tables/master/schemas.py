from pydantic import BaseModel

class SupplierMasterIn(BaseModel):
    supplier_code: str
    vendor_account: str
    name: str
    payment_terms: str
    delivery_address: str
    supplier_location: str
    supplier_contact: str
    credit_days: int
    credit_limit: float
    currency: str
    lead_time: int
    terms: str
