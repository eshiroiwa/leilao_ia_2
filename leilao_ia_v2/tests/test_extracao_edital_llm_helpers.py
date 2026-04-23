import leilao_ia_v2.services.extracao_edital_llm as ext


def test_deve_omitir_temperature_gpt5():
    assert ext._deve_omitir_temperature("gpt-5") is True
    assert ext._deve_omitir_temperature("gpt-5.1") is True


def test_deve_omitir_temperature_o_family():
    assert ext._deve_omitir_temperature("o3-mini") is True


def test_deve_omitir_temperature_gpt4():
    assert ext._deve_omitir_temperature("gpt-4o-mini") is False


def test_extrair_json_objeto_fence():
    raw = ext._extrair_json_objeto('```json\n{"tipo_imovel": "casa"}\n```')
    assert '"casa"' in raw


def test_extrair_json_objeto_sem_fence():
    raw = ext._extrair_json_objeto('prefixo {"a": 1} sufixo')
    assert raw.strip() == '{"a": 1}'
