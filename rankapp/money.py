"""金额工具：全程 Decimal，入库前统一为两位小数。"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

CENT = Decimal("0.01")


def D(value) -> Decimal:
    """把任意输入安全地转成 Decimal；不接受 NaN/Infinity。"""
    if isinstance(value, Decimal):
        d = value
    elif isinstance(value, int):
        d = Decimal(value)
    else:
        text = str(value).strip().replace(",", "").replace("¥", "").replace("￥", "")
        if text.startswith("+"):
            text = text[1:]
        try:
            d = Decimal(text)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("金额格式不正确：%r" % (value,)) from exc
    if not d.is_finite():
        raise ValueError("金额必须是有限数字：%r" % (value,))
    return d


def q2(value) -> Decimal:
    return D(value).quantize(CENT, rounding=ROUND_HALF_UP)


def money_str(value) -> str:
    return format(q2(value), "f")


def parse_int(value, field: str = "数量") -> int:
    if isinstance(value, bool):
        raise ValueError("%s必须是整数" % field)
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("%s必须是整数：%r" % (field, value)) from exc
    return number
