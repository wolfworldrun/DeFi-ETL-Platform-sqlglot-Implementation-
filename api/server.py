"""
api/server.py
Flask API server that runs the actual risk models and ETL pipeline,
serving real data to the frontend visualizations.

Usage:
    python api/server.py

Endpoints:
    GET /api/var        — Historical VaR & CVaR over 90-day simulated ETH returns
    GET /api/il         — Impermanent Loss curve across price ratios
    GET /api/mev        — MEV risk scores across recent blocks
    GET /api/transfers  — Hourly token transfer volume from ETL pipeline
    GET /api/health     — Health check
"""

import sys
from pathlib import Path

# Make sure src/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, jsonify
from flask_cors import CORS

from api.data_service import DataService

app = Flask(__name__)
CORS(app)  # Allow the HTML frontend to call the API from any origin

service = DataService()


@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "models": ["var", "il", "mev", "transfers"]})


@app.route("/api/var")
def var_endpoint():
    """
    Returns rolling 30-day VaR and CVaR on a $1M ETH position.
    Driven by src/models/risk_models.py HistoricalVaR.
    """
    data = service.get_var_data()
    return jsonify(data)


@app.route("/api/il")
def il_endpoint():
    """
    Returns IL%, LP value, and hold value across price ratios 0.1x–5x.
    Driven by src/models/risk_models.py ImpermanentLossModel.
    """
    data = service.get_il_data()
    return jsonify(data)


@app.route("/api/mev")
def mev_endpoint():
    """
    Returns MEV risk scores for a simulated block range.
    Driven by src/models/risk_models.py MEVExposureModel.
    """
    data = service.get_mev_data()
    return jsonify(data)


@app.route("/api/transfers")
def transfers_endpoint():
    """
    Returns hourly ERC-20 and ERC-721 transfer volumes.
    Driven by src/transform/etl_pipeline.py TokenTransferDecoder.
    """
    data = service.get_transfer_data()
    return jsonify(data)


if __name__ == "__main__":
    print("\n🚀 DeFi ETL API running at http://localhost:5000")
    print("   Endpoints: /api/health  /api/var  /api/il  /api/mev  /api/transfers\n")
    app.run(debug=True, port=5000)
