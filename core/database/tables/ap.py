from sqlalchemy import (Column, Integer, String, Float, Date, Boolean, JSON)
from core.database.tables.utils.commons import Base

import random
import string


def generate_unique_id():
    return "".join(random.choices(string.ascii_letters + string.digits, k=8))


class AssortmentPlan(Base):
    __tablename__ = "assortment_plan"

    assortment_id                          = Column(Integer, primary_key=True, autoincrement=True)
    unique_id                              = Column(String(8), unique=True, default=generate_unique_id)
    channel                                = Column(String(50), default="")
    country                                = Column(String(50), default="")
    region                                 = Column(String(50), default="")
    area                                   = Column(String(50), default="")
    city                                   = Column(String(50), default="")
    store                                  = Column(String(100), default="")
    season                                 = Column(String(100), default="")
    collection                             = Column(String(100), default="")
    department                             = Column(String(50), default="")
    category                               = Column(String(50), default="")
    family                                 = Column(String(100), default="")
    sub_family                             = Column(String(100), default="")
    dom_comm                               = Column(String(100), default="")
    Supplier                               = Column(String(100), default="")
    sub_category                           = Column(String(100), default="")
    extended_sub_category                  = Column(JSON, default=dict)
    Sub_category_supplier                  = Column(JSON, default=dict)
    assembly_code_nickname                 = Column(JSON, default=dict)
    end_of_life                            = Column(JSON, default=dict)
    sku_article                            = Column(String(100), default="")
    description                            = Column(String(100), default="")
    product_image                          = Column(String(100), default="")
    include_image                          = Column(Boolean)
    exclude_image                          = Column(Boolean)
    budget_year                            = Column(Integer, default=0)
    budget_season                          = Column(String(100), default="")
    historical_year                        = Column(Integer, default=0)
    historical_season                      = Column(String(100), default="")
    sale                                   = Column(String(100), default="")
    article_efficiency                     = Column(Float, default=0.0)
    speed                                  = Column(Float, default=0.0)
    terminal_stock                         = Column(Integer, default=0)
    margin                                 = Column(Float, default=0.0)
    sellthru_percent                       = Column(Float, default=0.0)
    markdown                               = Column(Float, default=0.0)
    returns                                = Column(Float, default=0.0)
    marketing_segment                      = Column(String(100), default="")
    internal_abc                           = Column(String(100), default="")
    core_range                             = Column(String(100), default="")
    key_look                               = Column(String(100), default="")
    store_module                           = Column(String(100), default="")
    quartile                               = Column(String(100), default="")
    sortimeter                             = Column(String(100), default="")
    global_performance                     = Column(String(100), default="")
    min_DV                                 = Column(Float, default=0.0)
    max_DV                                 = Column(Float, default=0.0)
    sectors                                = Column(String(100), default="")
    financial_year                         = Column(Integer, default=0)
    otb_year                               = Column(Integer, default=0)
    otb_percent                            = Column(Float, default=0.0)
    otb_vs_act_forecast_percent            = Column(Float, default=0.0)
    otb_vs_py_percent                      = Column(Float, default=0.0)
    otb_vs_ppy_percent                     = Column(Float, default=0.0)
    cost                                   = Column(Float, default=0.0)
    sku_count_with_kpi_distribution_score  = Column(Integer)
    otb_per_sku                            = Column(Float, default=0.0)
    sales_mix_percent                      = Column(Float, default=0.0)
    abc                                    = Column(JSON, default=dict)
    abc_mix_percent                        = Column(Float, default=0.0)
    article_efficiency                     = Column(Float, default=0.0)
    article_efficiency_mix_percent         = Column(Float, default=0.0)
    speed_of_sales                         = Column(Float, default=0.0)
    speed_mix_percent                      = Column(Float, default=0.0)
    selthru_percent                        = Column(Float, default=0.0)
    selthru_mix_percent                    = Column(Float, default=0.0)
    rank                                   = Column(JSON, default=dict)
    rank_points                            = Column(JSON, default=dict)
    weightage                              = Column(JSON, default=dict)
    kpi_distribution_value                 = Column(JSON, default=dict)
    coeff_score_for_dv_or_avg_kpi_dist_val = Column(Float, default=0.0)
    formulated_otb                         = Column(Float, default=0.0)
    formulated_otb_by_sku                  = Column(Float, default=0.0)
    formulated_units_to_buy_by_sku         = Column(Float, default=0.0)
    formulated_sku_count_with_score        = Column(Integer, default=0)
    formulated_sku_count                   = Column(Integer, default=0)
    sales_actuals                          = Column(String(100), default="")
    status                                 = Column(JSON, default=dict)
    sales_act_forecast                     = Column(String(100), default="")
    sales_budget                           = Column(String(100), default="")
    act_forecast_vs_budget_percent         = Column(Float, default=0.0)
    sales_ly                               = Column(String(100), default="")
    act_forecast_vs_ly_percent             = Column(Float, default=0.0)
    sales_ppy                              = Column(String(100), default="")
    act_forecast_vs_ppy_percent            = Column(Float, default=0.0)
    average_retail_price                   = Column(JSON, default=dict)
    units_buy_by_sku                       = Column(JSON, default=dict)
    total_sku_count                        = Column(JSON, default=dict)
    units_per_sku                          = Column(JSON, default=dict)
    sales_act_or_forecast_per_sku          = Column(JSON, default=dict)
    exchange_rate                          = Column(Float, default=0.0)
    slab_method                            = Column(String(100), default="")
    created_by                             = Column(Integer, nullable=False, default=0)
    created_on                             = Column(Date, nullable=False)
    approved_by                            = Column(Integer, default=0)
    approved_on                            = Column(Date)
    last_updated_by                        = Column(Integer, default=0)
    last_updated_on                        = Column(Date)
