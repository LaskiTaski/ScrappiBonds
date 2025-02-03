import datetime


def safe_float(value):
    try:
        if value:
            return float(value.replace("%", "").replace(",", "."))
        return None

    except (ValueError, TypeError):
        return None


def safe_int(value):
    try:
        if value:
            return int(float(value.replace(",", ".")))
        return None

    except (ValueError, TypeError):
        return None


def safe_date(value):
    if value:
        try:
            return datetime.datetime.strptime(value, '%d-%m-%Y').date()

        except (ValueError, TypeError):
            return None

    return None


def calculate_days_to_maturity(repayment_date):
    if repayment_date:
        return (repayment_date - datetime.date.today()).days
    return None
