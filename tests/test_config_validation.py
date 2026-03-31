import pytest

from core.config import validate_runtime_config


def _minimal(**overrides):
    base = {
        "source": {"type": "csv", "path": "data/x.csv"},
        "target": {"type": "duckdb", "table": "t1"},
        "load": {"mode": "append"},
    }
    base.update(overrides)
    return base


def test_validate_runtime_config_ok_append() -> None:
    validate_runtime_config(_minimal())


def test_replace_with_incremental_rejected() -> None:
    cfg = _minimal(
        load={
            "mode": "replace",
            "incremental": {"enabled": True, "watermark_column": "ts"},
        }
    )
    with pytest.raises(ValueError, match="replace.*incremental"):
        validate_runtime_config(cfg)


def test_upsert_requires_primary_key() -> None:
    cfg = _minimal(load={"mode": "upsert"})
    with pytest.raises(ValueError, match="Upsert mode requires"):
        validate_runtime_config(cfg)


def test_upsert_ok_with_primary_key() -> None:
    validate_runtime_config(
        _minimal(load={"mode": "upsert", "primary_key": "id"})
    )


def test_invalid_table_identifier() -> None:
    cfg = _minimal(target={"type": "duckdb", "table": "1bad"})
    with pytest.raises(ValueError, match="Invalid target.table"):
        validate_runtime_config(cfg)


def test_parquet_source_ok() -> None:
    validate_runtime_config(
        _minimal(source={"type": "parquet", "path": "data/x.parquet"})
    )


def test_http_source_ok() -> None:
    validate_runtime_config(
        _minimal(
            source={
                "type": "http",
                "url": "https://example.com/api",
                "method": "GET",
            }
        )
    )


def test_http_post_source_ok() -> None:
    validate_runtime_config(
        _minimal(
            source={
                "type": "http",
                "url": "https://example.com/api",
                "method": "POST",
                "body": {"x": 1},
                "allow_single_object": True,
            }
        )
    )


def test_http_page_query_source_ok() -> None:
    validate_runtime_config(
        _minimal(
            source={
                "type": "http",
                "url": "https://example.com/api",
                "method": "GET",
                "pagination": {
                    "enabled": True,
                    "strategy": "page_query",
                    "page_size": 10,
                    "max_pages": 5,
                },
            }
        )
    )


def test_http_page_query_requires_max_pages() -> None:
    cfg = _minimal(
        source={
            "type": "http",
            "url": "https://example.com/api",
            "method": "GET",
            "pagination": {
                "enabled": True,
                "strategy": "page_query",
                "page_size": 10,
            },
        }
    )
    with pytest.raises(ValueError, match="max_pages"):
        validate_runtime_config(cfg)


def test_unknown_source_type_rejected() -> None:
    cfg = _minimal(source={"type": "api", "path": "x"})
    with pytest.raises(ValueError, match="source.type must be"):
        validate_runtime_config(cfg)


def test_postgres_source_ok() -> None:
    validate_runtime_config(
        _minimal(
            source={
                "type": "postgres",
                "dsn": "postgresql://localhost:5432/db",
                "query": "SELECT id, name FROM public.orders LIMIT 10",
            }
        )
    )


def test_http_bearer_token_env_ok() -> None:
    validate_runtime_config(
        _minimal(
            source={
                "type": "http",
                "url": "https://example.com/api",
                "bearer_token_env": "API_TOKEN",
            }
        )
    )


def test_http_bearer_conflicts_with_headers_auth() -> None:
    cfg = _minimal(
        source={
            "type": "http",
            "url": "https://example.com/api",
            "headers": {"Authorization": "Bearer x"},
            "bearer_token_env": "API_TOKEN",
        }
    )
    with pytest.raises(ValueError, match="Authorization cannot be combined"):
        validate_runtime_config(cfg)


def test_postgres_requires_query() -> None:
    cfg = _minimal(
        source={
            "type": "postgres",
            "dsn": "postgresql://localhost:5432/db",
            "query": "",
        }
    )
    with pytest.raises(ValueError, match="source.query"):
        validate_runtime_config(cfg)
