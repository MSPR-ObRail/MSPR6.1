from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from . import database, models, schemas  

# Initialisation de FastAPI
app = FastAPI(
    title="ObRail API", 
    description="API pour le pipeline ObRail Europe",
    version="1.0.0"
)

# 1. Endpoint pour récupérer la liste complète des routes (format tableau)
@app.get("/routes", response_model=List[schemas.FactRoute], tags=["Routes"])
def get_routes(limit: int = 50, db: Session = Depends(database.get_db)):
    """
    Récupère la liste des routes avec les détails de CO2.
    """
    return db.query(models.FactRoute).limit(limit).all()

# 2. Endpoint pour le Dashboard (Statistiques générales)
@app.get("/stats", response_model=schemas.SummaryStats, tags=["Dashboard"])
def get_stats(db: Session = Depends(database.get_db)):
    """
    Récupère les statistiques agrégées pour le dashboard de Samy.
    """
    # Note: Ici il faudra faire un calcul (ou query une vue SQL)
    # Pour l'instant, on simule un retour avec des valeurs par défaut
    # car le modèle SQLAlchemy doit être adapté pour les calculs.
    return {
        "total_routes": db.query(models.FactRoute).count(),
        "countries_covered": 5, 
        "avg_distance_km": 450.5,
        "total_co2_saved_kg": 12000.0,
        "total_co2_saved_tons": 12.0,
        "avg_savings_percent": 15.5
    }