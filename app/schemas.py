from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime

# Configuration commune pour tous les schémas
class Config:
    from_attributes = True # Permet de lire les objets SQLAlchemy

# ========================================
# DIMENSIONS
# ========================================

class Country(BaseModel):
    country_code: str
    country_name: str
    class Config: from_attributes = True

class TransportMode(BaseModel):
    mode_id: int
    mode_name: str
    gco2_per_pkm: Optional[float]
    source: Optional[str]
    class Config: from_attributes = True

class TrainType(BaseModel):
    type_id: int
    type_code: str
    type_name: str
    class Config: from_attributes = True

# ========================================
# FACT ROUTE (La base pour Samy)
# ========================================

class FactRoute(BaseModel):
    route_id: int
    route_name: Optional[str]
    origin: str
    destination: str
    origin_country: Optional[str]
    destination_country: Optional[str]
    distance_km: Optional[float]
    train_co2_kg: Optional[float]
    co2_savings_kg: Optional[float]
    savings_percent: Optional[float]
    class Config: from_attributes = True

# ========================================
# ANALYTICS (Pour les vues SQL du schéma)
# ========================================

class SavingsByCountry(BaseModel):
    origin_country: str
    route_count: int
    total_savings_kg: float
    avg_savings_per_route_kg: float
    total_savings_tons: float
    class Config: from_attributes = True

class SummaryStats(BaseModel):
    total_routes: int
    countries_covered: int
    avg_distance_km: float
    total_co2_saved_kg: float
    total_co2_saved_tons: float
    avg_savings_percent: float
    class Config: from_attributes = True

class RoutesByType(BaseModel):
    train_type: str
    route_count: int
    avg_distance_km: float
    total_savings_kg: float
    class Config: from_attributes = True