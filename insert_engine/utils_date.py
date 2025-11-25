import datetime

def load_holidays(path: str) -> set:
    holidays = set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                try:
                    holidays.add(datetime.date.fromisoformat(s))
                except ValueError:
                    pass
    except FileNotFoundError:
        # Optional: print or just return empty set
        holidays = set()
    return holidays


def is_market_closed(d: datetime.date, holiday_set: set) -> bool:
    # 5 = Saturday, 6 = Sunday
    if d.weekday() >= 5:
        return True
    if d in holiday_set:
        return True
    return False
