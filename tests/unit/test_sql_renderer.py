from adapters.sql_renderer import get_sql_dialect


def test_postgres_date_bucket_month():
    dialect = get_sql_dialect("postgres")
    expr = dialect.render_date_bucket('f."created_at"', "month")
    assert "date_trunc('month'" in expr
    assert dialect.limit_placeholder == "%s"


def test_sqlite_date_bucket_year():
    dialect = get_sql_dialect("sqlite")
    expr = dialect.render_date_bucket("created_at", "year")
    assert "strftime('%Y-01-01'" in expr
    assert dialect.limit_placeholder == "?"


def test_mysql_date_bucket_day():
    dialect = get_sql_dialect("mysql")
    expr = dialect.render_date_bucket("created_at", "day")
    assert expr == "date(created_at)"
    assert dialect.limit_placeholder == "%s"
