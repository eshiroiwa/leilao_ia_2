from leilao_ia_v2.services.geo_medicao import coords_de_anuncio, geo_bucket_de_coords, haversine_km


def test_haversine_zero():
    assert haversine_km(-30.0, -51.0, -30.0, -51.0) == 0.0


def test_haversine_pequena_distancia():
    d = haversine_km(-30.0, -51.0, -30.045, -51.0)
    assert 4.0 < d < 6.0


def test_geo_bucket_formato():
    g = geo_bucket_de_coords(-23.55, -46.63)
    assert "_" in g and ("S" in g or "N" in g) and ("W" in g or "E" in g)


def test_coords_de_anuncio_colunas():
    r = {"latitude": -22.9, "longitude": -43.1}
    assert coords_de_anuncio(r) == (-22.9, -43.1)
