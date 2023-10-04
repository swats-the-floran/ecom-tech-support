"""Dataclasses representing data getting from elasticsearch api logs or feeds."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union


@dataclass
class Base:
    direction: str
    datetime: datetime
    org_name: str = ''


@dataclass
class StoreBase(Base):
    store_guid: str = ''
    store_id: str = ''
    # client: str = ''
    # city: str = ''
    # region: str = ''
    address: str = ''


@dataclass
class StoreBaseSchedule(StoreBase):
    deadline_date1: str = ''
    delivery_date1: str = ''
    deadline_date2: str = ''
    delivery_date2: str = ''
    deadline_date3: str = ''
    delivery_date3: str = ''


@dataclass
class Store1C(StoreBaseSchedule):
    b2b_price_guid: str = ''
    b2c_price_guid: str = ''
    # price_guid: str = ''
    hit_link: str = ''


@dataclass
class StoreStandard(StoreBaseSchedule):
    hit_link: str = ''


@dataclass
class StockBase(Base):
    quantity: int = 0
    product_identifier: str = ''
    expiration_date : str = ''


@dataclass
class Stock1C(StockBase):
    hit_link : str = ''


@dataclass
class StockStandard(StockBase):
    price: int = 0
    price_guid: Union[str, int] = ''
    region: str = ''
    price_type: str = ''
    hit_link : str = ''


@dataclass
class PriceBase(Base):
    product_identifier: str = ''


@dataclass
class Price1C(PriceBase):
    price_guid: str = ''
    price_type: str = ''
    vat: str = ''
    b2c_used: Optional[bool] = None
    price_inc_vat: int = 0
    price_wo_vat: int = 0
    price_promo: int = 0
    hit_link: str = ''


@dataclass
class PriceStandard(PriceBase):
    price: int = 0
    price_guid: Union[str, int] = ''
    price_type: str = ''
    region: Union[int, str] = ''
    quantity: int = 0
    expiration_date: str = ''
    hit_link: str = ''

