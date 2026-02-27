from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from . import database, models

# Initialisation de FastAPI
app = FastAPI(
    title="ObRail API", 
    description="API pour le pipeline ObRail Europe",
    version="1.0.0"
)

# Endpoint "Health Check"
@app.get("/health", tags=["System"])
def health_check(db: Session = Depends(database.get_db)):
    """
    Vérifie si l'API est en ligne et connectée à la base de données.
    """
    try:
        # On exécute une requête simple pour tester la connexion DB
        db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Database connection failed: {str(e)}"
        )

# Endpoint de test pour vérifier tes modèles (FactRoute)
@app.get("/routes/count", tags=["Routes"])
def get_routes_count(db: Session = Depends(database.get_db)):
    """
    Compte le nombre total de routes dans la table fact_routes.
    """
    try:
        count = db.query(models.FactRoute).count()
        return {"total_routes": count}
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error querying routes: {str(e)}"
        )