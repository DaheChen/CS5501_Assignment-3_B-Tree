# B+ Tree Transaction Index Assignment

This repository contains a manual Python B+ Tree implementation for transaction
data with these fields:

- `Transaction_ID`
- `Account Holder`
- `Transaction_Date`
- `Amount`
- `Beneficiary`

The primary index key is `Transaction_ID`. The script builds two trees:

1. Best-fit B+ Tree using a page-size-based fanout estimate.
2. Degree `m = 4` B+ Tree for comparison.

## Main Script

```powershell
python bplus_transaction_assignment.py --dataset "transactions_100k.csv" --output-dir outputs
```

The script performs:

- dataset validation and cleaning
- Best-fit degree estimation
- Best-fit B+ Tree construction
- degree-4 B+ Tree construction
- milestone height/width measurement
- JSON export
- post-export JSON reload validation
- 1000-key random search experiment
- CSV tables and PNG chart generation

## Important Implementation Notes

- Internal nodes store separator keys and child pointers.
- Leaf nodes store full transaction records.
- Leaf nodes are linked through `next_leaf` for sequential/range scanning.
- Duplicate `Transaction_ID` values are stored under the same leaf-key bucket.
- The Best-fit degree is a practical page-size-based fanout estimate, not a
  universal theoretical optimum.
- Date and amount range queries are not optimized by the `Transaction_ID` tree;
  they would require secondary B+ Tree indexes.
- Measured search time is in-memory Python wall-clock time, not disk I/O time.

## Verified Results

Best-fit degree estimate:

| Parameter | Value |
|---|---:|
| Page size | 4096 bytes |
| Key size | 16 bytes |
| Pointer size | 8 bytes |
| Best-fit max degree | 171 |

Best-fit milestone statistics:

| Insertion | Records | Height | Width by Level | Internal Nodes | Leaf Nodes | Total Nodes |
|---|---:|---:|---|---:|---:|---:|
| 25% | 25,000 | 3 | `[1, 2, 213]` | 3 | 213 | 216 |
| 50% | 50,000 | 3 | `[1, 4, 431]` | 5 | 431 | 436 |
| 75% | 75,000 | 3 | `[1, 4, 637]` | 5 | 637 | 642 |
| 100% | 100,000 | 3 | `[1, 8, 850]` | 9 | 850 | 859 |

Final tree comparison:

| Tree | Max Degree | Height | Total Records |
|---|---:|---:|---:|
| Best-fit B+ Tree | 171 | 3 | 100,000 |
| Degree-4 B+ Tree | 4 | 11 | 100,000 |

Search experiment:

| Tree | Sample Size | Found | Avg Time ms | Avg Node Accesses |
|---|---:|---:|---:|---:|
| Best-fit B+ Tree | 1000 | 1000 | 0.004789 | 3.0 |
| Degree-4 B+ Tree | 1000 | 1000 | 0.011069 | 11.0 |

## Output Files Tracked in Git

The repository tracks lightweight audit outputs:

- `outputs/*.csv`
- `outputs/*.png`

The raw JSON tree exports are intentionally ignored by `.gitignore` because:

- `outputs/best_fit_bplus_tree.json` is about 42.8 MB.
- `outputs/degree_4_bplus_tree.json` is about 116.6 MB and exceeds GitHub's
  100 MB single-file limit.

Regenerate JSON files locally by rerunning the script.
