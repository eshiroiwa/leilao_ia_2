"""Distância geográfica e bucket de grid (alinhado ao legado ~550 m por célula)."""

from __future__ import annotations

import math
from typing import Any, Optional


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância em quilómetros entre dois pontos WGS84."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlamb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlamb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def geo_bucket_de_coords(lat: float, lon: float, passo_graus: float = 0.005) -> str:
    """Grid simples (~550 m em latitude com passo 0,005°)."""
    if passo_graus <= 0:
        passo_graus = 0.005
    lat_b = round(round(lat / passo_graus) * passo_graus, 4)
    lon_b = round(round(lon / passo_graus) * passo_graus, 4)
    lat_h = "N" if lat_b >= 0 else "S"
    lon_h = "E" if lon_b >= 0 else "W"
    return f"{lat_h}{abs(lat_b):.4f}_{lon_h}{abs(lon_b):.4f}"


def _parse_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def coords_de_anuncio(row: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    lat = _parse_float(row.get("latitude"))
    lon = _parse_float(row.get("longitude"))
    if lat is not None and lon is not None:
        return lat, lon
    md = row.get("metadados_json")
    if isinstance(md, dict):
        lat2 = _parse_float(md.get("latitude") or md.get("lat"))
        lon2 = _parse_float(md.get("longitude") or md.get("lon"))
        if lat2 is not None and lon2 is not None:
            return lat2, lon2
    return lat, lon
