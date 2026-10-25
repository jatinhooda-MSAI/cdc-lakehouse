import re
import sys

from common import NAMESPACE, catalog, log

L = log("resolve")
USAGE = "python -m controller.resolve TABLE rename OLD NEW | TABLE drop COLUMN | TABLE keep COLUMN"


def main(argv):
    if len(argv) < 3:
        sys.exit(USAGE)
    table, action, args = argv[0], argv[1], argv[2:]
    tbl = catalog().load_table((NAMESPACE, table))
    halted = tbl.properties.get("cdc.halted", "")
    m = re.match(r"v(\d+) removed", halted)
    if not m:
        sys.exit(f"{table} is not halted on a removed column: {halted or 'not halted'}")
    if action == "rename":
        with tbl.update_schema() as u:
            u.rename_column(args[0], args[1])
    elif action == "drop":
        with tbl.update_schema() as u:
            u.delete_column(args[0])
    elif action != "keep":
        sys.exit(USAGE)
    tbl = catalog().load_table((NAMESPACE, table))
    done = {x for x in tbl.properties.get("cdc.resolved-versions", "").split(",") if x} | {m.group(1)}
    with tbl.transaction() as tx:
        tx.set_properties({"cdc.resolved-versions": ",".join(sorted(done, key=int))})
        tx.remove_properties("cdc.halted")
    L.info("%s: %s %s, version %s resolved", table, action, args, m.group(1))


if __name__ == "__main__":
    main(sys.argv[1:])
