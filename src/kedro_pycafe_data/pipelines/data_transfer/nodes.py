import ibis.expr.types as ir


def identity(tbl: ir.Table) -> ir.Table:
    return tbl
