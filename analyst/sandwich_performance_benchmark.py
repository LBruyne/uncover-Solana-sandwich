import csv
import re
import sys
from collections import OrderedDict


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python3 sandwich_performance_benchmark.py <input.log> <output.csv>"
        )
        sys.exit(1)

    in_path = sys.argv[1]
    out_path = sys.argv[2]

    rows = []
    header_order = ["time"]  # always put timestamp first
    seen_keys = set(header_order)

    # Pre-compile a couple of light regexes
    time_re = re.compile(r"time=([^\s]+)")
    # We'll split on the first occurrence of 'msg=Benchmark:' to get the key=val part
    marker = "msg=Benchmark:"

    with open(in_path, "r", encoding="utf-8") as f:
        for line in f:
            if marker not in line:
                continue

            # Extract timestamp (keep raw ISO string)
            tm = time_re.search(line)
            tstr = tm.group(1) if tm else ""

            # Everything after 'msg=Benchmark:' are space-separated key=value pairs
            kv_part = line.split(marker, 1)[1].strip()

            # Build an ordered row dict
            row = OrderedDict()
            row["time"] = tstr

            # Parse k=v tokens separated by spaces
            for tok in kv_part.split():
                if "=" not in tok:
                    continue
                k, v = tok.split("=", 1)

                # Record the key; preserve first-seen column order
                if k not in seen_keys:
                    header_order.append(k)
                    seen_keys.add(k)

                row[k] = v

            rows.append(row)

    # Write CSV with all discovered columns; fill missing with empty string
    with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header_order)
        writer.writeheader()
        for r in rows:
            # Ensure all keys exist for this row
            for k in header_order:
                if k not in r:
                    r[k] = ""
            writer.writerow(r)


if __name__ == "__main__":
    main()
