from src.core.constants import IDENTIFIER_PATTERN, QUALIFIED_IDENTIFIER_PATTERN


def validate_identifier(identifier: str, allow_qualified: bool = False) -> bool:
    """
    Валидирует SQL идентификатор для предотвращения injection.
    :param identifier: Идентификатор для валидации
    :param allow_qualified: Разрешить квалифицированные имена
    :return: True если идентификатор валиден, иначе False"""
    if not identifier:
        return False

    pattern = QUALIFIED_IDENTIFIER_PATTERN if allow_qualified else IDENTIFIER_PATTERN
    return bool(pattern.match(identifier))
