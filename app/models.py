from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, ForeignKey, func
from sqlalchemy.orm import relationship
from .database import Base

# ========================================
# DIMENSION TABLES
# ========================================

class DimCountry(Base):
    __tablename__ = "dim_countries"
    
    country_code = Column(String(5), primary_key=True)
    country_name = Column(String(100))

class DimTransportMode(Base):
    __tablename__ = "dim_transport_modes"
    
    mode_id = Column(Integer, primary_key=True, autoincrement=True)
    mode_name = Column(String(50), nullable=False)
    gco2_per_pkm = Column(Numeric(10, 2))
    source = Column(String(100))

class DimTrainType(Base):
    __tablename__ = "dim_train_types"
    
    type_id = Column(Integer, primary_key=True, autoincrement=True)
    type_code = Column(String(20), nullable=False, unique=True)
    type_name = Column(String(50), nullable=False)

# ========================================
# FACT TABLE - ROUTES
# ========================================

class FactRoute(Base):
    __tablename__ = "fact_routes"
    
    route_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Route identification
    route_name = Column(String(200))
    route_name_simple = Column(String(200))
    
    # Origin and destination
    origin = Column(String(100), nullable=False)
    destination = Column(String(100), nullable=False)
    
    # Foreign keys
    origin_country = Column(String(5), ForeignKey("dim_countries.country_code"))
    destination_country = Column(String(5), ForeignKey("dim_countries.country_code"))
    
    # Route characteristics
    distance_km = Column(Numeric(10, 2))
    service_type = Column(String(20))
    train_type = Column(String(20))
    
    # Emission factors
    train_gco2_pkm = Column(Numeric(10, 2))
    plane_gco2_pkm = Column(Numeric(10, 2))
    
    # Calculated emissions
    train_co2_kg = Column(Numeric(10, 2))
    plane_co2_kg = Column(Numeric(10, 2))
    co2_savings_kg = Column(Numeric(10, 2))
    savings_percent = Column(Numeric(5, 2))
    
    # Metadata
    emission_source = Column(String(100))
    calculation_date = Column(Date)
    created_at = Column(DateTime, server_default=func.now())

    # Relationships (optionnel, facilite les jointures)
    origin_country_rel = relationship("DimCountry", foreign_keys=[origin_country])
    destination_country_rel = relationship("DimCountry", foreign_keys=[destination_country])