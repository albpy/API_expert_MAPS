import re
from datetime import datetime, timedelta
from json import dumps as dump_to_json_str
from threading import Thread
from typing import Callable, Dict, List, Literal, Tuple

import pandas as pd
from requests import post as post_request

from core.database.tables import ItemMaster as IM_Model
from core.database.tables import MasterDataModelProtocol
from core.database.tables import PurchaseOrder as PO_Model
from core.database.tables import SalesRecords as SR_Model
from core.database.tables import StoreMaster as SM_Model
from core.database.tables import SupplierMaster as SPM_Model
from core.tools.algorithm import SortedSet
from core.tools.indexer import GlobalIndexer, where


def create_regex_pattern(string_list: list):
    # Escape special characters in each string and join them with the '|' operator
    pattern = '|'.join(re.escape(s) for s in string_list)
    
    # Create the regex pattern to match any of the strings in the list
    regex_pattern = f"({pattern})"
    
    return regex_pattern

class UpdateWorker:
    def __init__(self, master_index):
        # Indexes
        self.master_index = master_index

        self._item_master_index     = self.master_index.ItemMaster_Index
        # self._store_master_index    = self.master_index.index('store_master', cache_size=None)
        # self._supplier_master_index = self.master_index.index('supplier_master', cache_size=None)
        # self._po_history_index      = self.master_index.index('po_history', cache_size=None)

        self.__dynamic_365_access_token = ""
                
    def __get_dynamic_365_token(self):
        print(f"[INFO] Fetching Access Token from Microsoft...")
        url = "https://login.windows.net/58191332-4582-42f1-a685-f77f77def707/oauth2/token"

        payload = {
            'grant_type': 'client_credentials',
            'client_Id': 'f88a8ff0-a5d6-4924-b449-5d3810fc515a',
            'client_secret': 'ItO8Q~nzaDbXc2qq0ShJYadR.mmP_S~4XFpSibcC',
            'resource': 'https://btech-erp.operations.dynamics.com'
        }
        files=[]
        headers = {
            'Cookie': 'fpc=Ahxk9y5L9VZNi38rRAqDAn-1GuQ_AQAAAKEOktwOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
        }

        response = post_request(url, headers=headers, data=payload, files=files)
        
        if response.status_code == 200:
            resp_data = response.json()
            self.__dynamic_365_access_token = f"Bearer {resp_data['access_token']}"

    def fetch_item_master_data(self):
        self.__get_dynamic_365_token() # Get new token from Dynamic 365
        print(f"[INFO] Fetching Item Master Records")
        url = "https://btech-erp.operations.dynamics.com/api/services/Update_Item_MasterServiceGroup/Update_Item_MasterServices/getItemInformation"
        headers = {
            "Authorization": self.__dynamic_365_access_token
        }
        payload = dump_to_json_str({})

        response = post_request(url=url, headers=headers, data=payload)

        if response.status_code == 200:
            resp_data = response.json()
            item_master_data = resp_data["ListData"]
            data_to_upload = []
            for idx, imd in enumerate(item_master_data):
                im_record = {}
                im_record["item_id"] = idx
                try:    im_record["primary_vendor_id"]      = imd["PrimaryVendorId"]
                except: im_record["primary_vendor_id"]      = ""
                try:    im_record["lead_time"]              = imd["LeadTime"]
                except: im_record["lead_time"]              = 1
                try:    im_record["av_cost"]                = imd["AVCost"]
                except: im_record["av_cost"]                = 0.0
                try:    im_record["new_lookup_code"]        = imd["NewLookupCode"]
                except: im_record["new_lookup_code"]        = ""
                try:    im_record["companyabc"]             = imd["CompanyABC"]
                except: im_record["companyabc"]             = ""
                try:    im_record["family_abc"]             = imd["Family_ABC"]
                except: im_record["family_abc"]             = ""
                try:    im_record["item_lookup_code"]       = imd["ItemLookupCode"]
                except: im_record["item_lookup_code"]       = ""
                try:    im_record["lookup_code"]            = imd["LookupCode"]
                except: im_record["lookup_code"]            = ""
                try:    im_record["description"]            = imd["Description"]
                except: im_record["description"]            = ""
                try:    im_record["department"]             = imd["Department"]
                except: im_record["department"]             = ""
                try:    im_record["category_name"]          = imd["CategoryName"]
                except: im_record["category_name"]          = ""
                try:    im_record["family"]                 = imd["Family"]
                except: im_record["family"]                 = ""
                try:    im_record["sub_family"]             = imd["subFamily"]
                except: im_record["sub_family"]             = ""
                try:    im_record["supplier"]               = imd["Supplier"]
                except: im_record["supplier"]               = ""
                try:    im_record["sub_category"]           = imd["SubCategory"]
                except: im_record["sub_category"]           = ""
                try:    im_record["extended_sub_category"]  = imd["ExtendedSubCategory1"]
                except: im_record["extended_sub_category"]  = ""
                try:    im_record["sub_category_supplier"]  = imd["SubCategorySupplier"]
                except: im_record["sub_category_supplier"]  = ""
                try:    im_record["assembly_code_nickname"] = imd["AssemblyCodeNickname"]
                except: im_record["assembly_code_nickname"] = ""
                try:    im_record["status"]                 = True if str(imd["Status"]).strip().upper()=="YES" else False
                except: im_record["status"]                 = ""
                try:    im_record["dom_comm"]               = imd["DOM_COMM"]
                except: im_record["dom_comm"]               = ""
                try:    im_record["item_type"]              = imd["ItemType"]
                except: im_record["item_type"]              = 0
                try:    im_record["company_abc"]            = imd["Company_ABC"]
                except: im_record["company_abc"]            = ""
                try:    im_record["bom_unit_id"]            = imd["BOMUnitId"]
                except: im_record["bom_unit_id"]            = ""
                try:    im_record["alt_invent_size_id"]     = imd["AltInventSizeId"]
                except: im_record["alt_invent_size_id"]     = ""
                try:    im_record["alt_invent_color_id"]    = imd["AltInventColorId"]
                except: im_record["alt_invent_color_id"]    = ""
                try:    im_record["height"]                 = imd["Height"]
                except: im_record["height"]                 = 0.0
                try:    im_record["width"]                  = imd["Width"]
                except: im_record["width"]                  = 0.0
                try:    im_record["unit_volume"]            = imd["UnitVolume"]
                except: im_record["unit_volume"]            = 0.0
                try:    im_record["net_weight"]             = imd["NetWeight"]
                except: im_record["net_weight"]             = 0.0
                data_to_upload.append(im_record)
            
            return data_to_upload
        return []
    
    def fetch_supplier_master_data(self):
        url = "https://btech-erp.operations.dynamics.com/api/services/Update_SuppliersServiceGroup/Update_SuppliersService/getSupplierInfon"
        headers = {
            "Authorization": self.__dynamic_365_access_token
        }
        payload = dump_to_json_str({})

        response = post_request(url=url, headers=headers, data=payload)


        if response.status_code == 200:
            resp_data = response.json()
            return resp_data["ListData"]
        return None

    def fetch_sales_records_data(self):
        self.__get_dynamic_365_token() # Get new token from Dynamic 365
        # Get today's date
        today = datetime.today()
        # Calculate yesterday's date by subtracting one day
        yesterday = today - timedelta(days=1)
        yesterdays_date = yesterday.strftime("%m/%d/%Y")
        url = "https://btech-erp.operations.dynamics.com/api/services/getSalesUpdatesServiceGroup/SalesUpdateService/getSalesUpdates"
        headers = {
            "Authorization": self.__dynamic_365_access_token
        }
        payload = dump_to_json_str({"_salesParm":{"From_Date":yesterdays_date,"To_Date":yesterdays_date}})

        response = post_request(url=url, headers=headers, data=payload)

        if response.status_code == 200:
            response_data = response.json()
            sales_data_list = response_data["ListData"]
            data_to_upload = []
            for record in sales_data_list:
                sales_record = {}
                try:    sales_record["source_store_name"] = record["Source_store_Name"]
                except: sales_record["source_store_name"] = ""
                try:    sales_record["order_store"]       = record["Order_Store"]
                except: sales_record["order_store"]       = ""
                try:    sales_record["trx_store_id"]      = record["TRX_Store_id"]
                except: sales_record["trx_store_id"]      = ""
                try:    sales_record["trx_store_name"]    = record["TRX_Store_Name"]
                except: sales_record["trx_store_name"]    = ""
                try:    sales_record["customer_type"]     = record["Customer_Type"]
                except: sales_record["customer_type"]     = ""
                try:    sales_record["trx_id"]            = record["TRX_ID"]
                except: sales_record["trx_id"]            = ""
                try:    sales_record["sold_date"]         = datetime.strptime(record["Sold_Date"][:10], "%Y-%m-%d").date()
                except: sales_record["sold_date"]         = yesterday
                try:    sales_record["item_code"]         = record["Item_Code"]
                except: sales_record["item_code"]         = ""
                try:    sales_record["description"]       = record["Description"]
                except: sales_record["description"]       = ""
                try:    sales_record["department_name"]   = record["Department_Name"]
                except: sales_record["department_name"]   = ""
                try:    sales_record["category_name"]     = record["Category_Name"]
                except: sales_record["category_name"]     = ""
                try:    sales_record["family"]            = record["Family"]
                except: sales_record["family"]            = ""
                try:    sales_record["dom_comm"]          = record["DOM_COMM"]
                except: sales_record["dom_comm"]          = ""
                try:    sales_record["supplier"]          = record["Supplier"]
                except: sales_record["supplier"]          = ""
                try:    sales_record["sub_category"]      = record["Sub_Category"]
                except: sales_record["sub_category"]      = ""
                try:    sales_record["extd_sub_category"] = record["Extended_Sub_Category"]
                except: sales_record["extd_sub_category"] = ""
                try:    sales_record["quantity"]          = record["Quantity"]
                except: sales_record["quantity"]          = 0
                try:    sales_record["full_discount"]     = record["Full_Discount"]
                except: sales_record["full_discount"]     = ""
                try:    sales_record["net_sales"]         = record["Net_Sales"]
                except: sales_record["net_sales"]         = 0.0
                try:    sales_record["sales_tax"]         = record["SalesTax"]
                except: sales_record["sales_tax"]         = 0.0
                try:    sales_record["gross_sales"]       = record["Gross_Sales"]
                except: sales_record["gross_sales"]       = 0.0
                try:    sales_record["total_sales_price"] = record["Total_Sales_Price"]
                except: sales_record["total_sales_price"] = 0.0
                try:    sales_record["full_price"]        = record["Full_Price"]
                except: sales_record["full_price"]        = 0.0
                try:    sales_record["item_cost"]         = record["Item_Cost"]
                except: sales_record["item_cost"]         = 0.0
                
                data_to_upload.append(sales_record)
            return data_to_upload
        return []
    
    def fetch_purchase_order_history(self):
        # TODO: IMPLEMENT POH FLOW
        raise NotImplementedError("Method not available")

    # Utility Functions
    def __update_table(self, data_model: MasterDataModelProtocol, update_data: List[Dict], data_title: str, truncate: bool=True):
        print(f"[INFO] Updating {data_title} table")
        if truncate: 
            data_model.truncate_table()
        data_model.bulk_insert(update_data)
        print(f"[INFO] Finished updating {data_title} table")

    def __update_index(self, data_index: Callable, update_data: List[Dict], data_title: str, indexed_cols: dict):
        def delete_unclassified_rows(input_df: pd.DataFrame):
            # Drop all rows which don't have the nacessary column values
            filter_items  = input_df[indexed_cols["non_nullable"]]
            no_family     = filter_items[filter_items.family=='']
            no_sub_family = no_family[no_family.sub_family=='']
            no_cat        = no_sub_family[no_sub_family.category_name=='']
            no_sub_cat    = no_cat[no_cat.sub_category=='']

            for item_id in list(no_sub_cat["item_id"].unique()):
                input_df.drop(input_df[input_df['item_id'] == item_id].index, inplace=True)

            return input_df
        
        # Convert to a DataFrame
        df = pd.DataFrame(update_data)
        # Take nacessary columns only for indexing...
        df = df[indexed_cols["non_nullable"]+indexed_cols["others"]]
        filtered_df = delete_unclassified_rows(input_df=df)

        print(f"[INFO] Updating {data_title} index")
        data_index.truncate()
        data_index.insert_multiple(filtered_df.to_dict("records"))
        print(f"[INFO] Finished updating {data_title} index")

    def __update_data(self, fetcher_fn: Callable, model_fn: MasterDataModelProtocol, index_fn: Callable, indexed_cols: dict={}, truncate_table: bool=False, title: str=""):
        _data_from_api = fetcher_fn()
        if model_fn: self.__update_table(model_fn, _data_from_api, title, truncate_table) 
        if index_fn: self.__update_index(index_fn, _data_from_api, title, indexed_cols)

    def run_updates(self, update_config=Dict[str, Tuple[Literal["table"], ...]]):
        update_recipe = {
            "item_master": {
                "model": IM_Model, "index": self._item_master_index, "fetcher": self.fetch_item_master_data, "truncate": True,
                "indexed_cols": {
                    "non_nullable": [ "item_id", "family", "sub_family", "category_name", "sub_category" ],
                    "others": [ "supplier", "item_lookup_code" ]
                }
            }, "sales_history": {
                "model": SR_Model, "index": None, "fetcher": self.fetch_sales_records_data, "truncate": False, "indexed_cols": {}
            }, "store_master": {
                "model": SM_Model, "index": None, "fetcher": self.fetch_supplier_master_data, "truncate": True, "indexed_cols": {}
            }, "po_history": {
                "model": PO_Model, "index": None, "fetcher": self.fetch_purchase_order_history, "truncate": False, "indexed_cols": {}
            }
        }
        
        for master_title, master_config in update_config.items():
            print(master_config)
            self.__update_data(
                fetcher_fn     = update_recipe[master_title]["fetcher"], 
                model_fn       = update_recipe[master_title]["model"] if "table" in master_config else None, 
                index_fn       = update_recipe[master_title]["index"] if "index" in master_config else None, 
                indexed_cols   = update_recipe[master_title]["indexed_cols"],
                truncate_table = update_recipe[master_title]["truncate"], 
                title          = " ".join([_str.capitalize() if len(_str)>2 else _str.upper() for _str in  master_title.split("_")])
            )

class MasterData_manager:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MasterData_manager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.master_filters = { "channel": [], "category": [], "sub_category": [], "family": [], "sub_family": [], "suppliers": [], "sku": [], "class": [], "top_items": [50, 100, 150, 200, 250] }

            self.__indexObj = GlobalIndexer()

            self._item_master_index = self.__indexObj.ItemMaster_Index

            self.__update_worker = UpdateWorker(master_index=self.__indexObj)

            self._initialized = True
        
    def update_master_data(self, update_config):
        print("[INFO] Running master data updates in background...")
        t = Thread(target=self.__update_worker.run_updates(update_config=update_config))
        t.start()
    
    def updateMasterDataFilterSet(self):
        self.master_filters = {
            "channel": [],
            "category": [],
            "sub_category": [],
            "family": [],
            "sub_family": [],
            "suppliers": [],
            "sku": [],
            "class": [],
            "top_items": [50, 100, 150, 200, 250]
        }

        try:    self.master_filters["channel"] = ["Online", "Retail", "Tech Club", "Noon", "Showroom"]
        except: print(f"[WARN] Couldn't fill channels data")

        df = pd.DataFrame(self._item_master_index.all())

        # You might be wondering, 

        # If we already have all the data in the database, 
        # why the fuck we are maintaining an Indexer and then again storing data in the memory?

        # Simple explaination to smart solution: This is a hierarchical storage system:
        # 1. We store data in Database as a safe storage, which is slowest to retrieve from.
        # 2. We are indexing a subset of data in Redis, which can still run relational queries, 
        #     thus maintaining a elastic search layer in the syatem. This is faster than Database.
        # 3. We are further taking a subset of indexed data, which will be exchanged frequently between the frontend and backend.
        #     This set of data is supposed to be accessible within milliseconds and persisted.
        #     This method is the fastest.

        # Still didn't understand? You might have a small brain. No worries, go read some books.

        try: 
            ss = SortedSet(df["category_name"])
            ss.discard("")
            self.master_filters["category"] = list(ss)
        except: print(f"[WARN] Couldn't fill category data")

        try: 
            ss = SortedSet(df["sub_category"])
            ss.discard("")
            self.master_filters["sub_category"] = list(ss)
        except: print(f"[WARN] Couldn't fill sub category data")

        try: 
            ss = SortedSet(df["family"])
            ss.discard("")
            self.master_filters["family"] = list(ss)
        except: print(f"[WARN] Couldn't fill family data")

        try: 
            ss = SortedSet(df["sub_family"])
            ss.discard("")
            self.master_filters["sub_family"] = list(ss)
        except: print(f"[WARN] Couldn't fill sub family data")

        try: 
            ss = SortedSet(df["supplier"])
            ss.discard("")
            self.master_filters["suppliers"] = list(ss)
        except: print(f"[WARN] Couldn't fill suppliers data")

        try: 
            ss = SortedSet(df["item_lookup_code"])
            ss.discard("")
            self.master_filters["sku"] = list(ss)
        except: print(f"[WARN] Couldn't fill sku data")

        try:    self.master_filters["class"] = ["A", "B", "C"]
        except: print(f"[WARN] Couldn't fill class data")

        try:    self.master_filters["top_items"] = [50, 100, 150, 200, 250]
        except: print(f"[WARN] Couldn't fill top items data")

    def get_item_master_filter_data(self, filter_params: List[str]=[]):
        filter_params_options = ["channel", "category", "sub_category", "family", "sub_family", "suppliers", "sku", "class", "top_items"]

        if "all" in filter_params:
            return self.master_filters
        
        filter_values = {}
        for fp in filter_params:
            if isinstance(fp, str):
                _fp = fp.strip().lower()
                if _fp in filter_params_options:
                    filter_values[_fp] = self.master_filters[_fp]
        
        return filter_values
    
    # Hierarchical Filters...
    def family2subfamily(self, prod_family_list:List[str]=[]):
        _sub_family_list = self._item_master_index.search(
            where("family").matches(create_regex_pattern(prod_family_list)),
            return_keys=["sub_family"],
            orient="list",
            unique=True
        )
        
        if not len(_sub_family_list): return []
        sub_family_list=_sub_family_list["sub_family"]

        if "" in sub_family_list: sub_family_list.remove("")

        return sub_family_list
    
    def category2subcategory(self, prod_category_list:List[str]=[]):
        _sub_cat_list = self._item_master_index.search(
            where("category_name").matches(create_regex_pattern(prod_category_list)),
            return_keys=["sub_category"],
            orient="list",
            unique=True
        )

        if not len(_sub_cat_list): return []
        sub_cat_list=_sub_cat_list["sub_category"]
        
        if "" in sub_cat_list: sub_cat_list.remove("")
        
        return sub_cat_list
    
    def apply_hierarchical_filter(self, filter_name, filter_data):
        if filter_name == "category2sub_category":
            return self.category2subcategory(prod_category_list=filter_data)
        elif filter_name == "family2sub_family":
            return self.family2subfamily(prod_family_list=filter_data)
        else:
            return []
