from urllib.parse import parse_qs, urlparse


def parse_trino_jdbc(jdbc_url: str) -> dict:
    """
    Парсит JDBC URL Trino и возвращает dict с параметрами для подключения.
    Пример входа: jdbc:trino://host:443?user=foo&password=bar

    :param jdbc_url: строка подключения jdbc
    :returns:
    {
        "host": "host",
        "port": 443,
        "user": "foo",
        "password": "bar",
        **params
    }
    """
    if not jdbc_url.startswith("jdbc:trino://"):
        raise ValueError("невалидная строка Trino JDBC")

    url = jdbc_url[len("jdbc:") :]
    parsed = urlparse(url)

    if not parsed.hostname or not parsed.port:
        raise ValueError("отсутствует host или port в jdbc строке.")

    params = parse_qs(parsed.query)
    params = {k: v[0] for k, v in params.items()}

    if "user" not in params:
        raise ValueError("trino jdbc строка должна содержать user.")

    return {
        "host": parsed.hostname,
        "port": parsed.port,
        "user": params.get("user"),
        "password": params.get("password", None),
        **params,
    }
