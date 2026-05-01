# B+ Tree Transaction Index: Design Plan, Reviewer Notes, and Report Text

## Part 1: Design Plan with Reviewer Comments

| Component | Goal | Design | Reasoning | Reviewer Concern | Improvement / Control |
|---|---|---|---|---|---|
| 1. Dataset loading and cleaning | Read transaction data without corrupting the index input. | The script supports `.csv`, `.xlsx`, and `.xls`, strips column names, validates the five required fields, removes rows with missing `Transaction_ID`, parses dates, and converts amounts to numeric values. | An index is only meaningful if keys are consistent and records are valid enough to compare and serialize. | A reviewer may object if invalid rows disappear silently or if identifier formatting is changed. | The script prints cleaning metadata, preserves leading-zero IDs as strings, and reports duplicates/invalid dates/invalid amounts. |
| 2. B+ Tree node design | Separate routing from stored data. | Internal nodes store separator keys and child pointers; leaf nodes store keys and full transaction-record buckets. | This matches B+ Tree semantics: internal nodes guide search, leaves hold data. | Confusing B-tree and B+ Tree would weaken the explanation. | The implementation stores data only in leaves and validates that all leaves are at the same depth. |
| 3. Insertion and split logic | Keep the tree balanced while inserting records. | Insert into the correct leaf, split overflowing leaves, promote the first key of the right leaf, and recursively split internal nodes if needed. | Splitting is the mechanism that preserves logarithmic height growth. | The exact shape depends on insertion order and split policy. | The report states the split policy and reports measured shape rather than claiming a unique tree. |
| 4. Search logic | Find a unique transaction by `Transaction_ID`. | Binary search is used inside nodes, and the search follows separator keys to one leaf. | For fanout `m`, traversal takes `O(log_m N)` levels. | Timing alone can be noisy in Python. | The experiment reports both wall-clock time and average node accesses/comparisons. |
| 5. Linked leaves and range scans | Support sequential and range access. | Each leaf has a `next_leaf` pointer; `range_scan` finds the starting leaf and walks forward. | B+ Trees are good for range queries because sorted records are contiguous at the leaf level. | A `Transaction_ID` tree does not directly optimize date or amount ranges. | The report explicitly says date and amount range queries normally require secondary B+ Tree indexes. |
| 6. Best-fit max degree estimation | Choose a practical fanout rather than an arbitrary theoretical value. | The script uses `floor((page_size + key_size) / (key_size + pointer_size))`. With 4096-byte pages, 16-byte keys, and 8-byte pointers, this gives `m = 171`. | The formula follows the internal-page capacity model `m*pointers + (m-1)*keys <= page_size`. | This is approximate, not a universal database optimum. | The text calls it a "page-size-based practical fanout estimate" and lists omitted database details. |
| 7. Degree-4 tree construction | Build the required low-degree comparison tree. | The same records are inserted into a second tree with `max_degree = 4`. | This gives a small fanout tree that is easier to visualize and should have more levels. | It is not realistic for disk pages. | The report frames `m=4` as educational, not production-like. |
| 8. Height and width measurement | Show how the best-fit tree grows at 25%, 50%, 75%, and 100%. | Breadth-first traversal counts nodes at each level, internal nodes, leaf nodes, and total nodes. | Width by level is more informative than a single node count because it shows where growth occurs. | Results depend on insertion order. | The report states that the dataset order and split policy influence intermediate widths. |
| 9. JSON serialization | Export both trees for reproducibility and inspection. | The script serializes metadata and a recursive root structure; leaf links are represented by node IDs. | JSON makes the produced tree auditable without Python object pointers. | Datetime and numpy values may be unserializable. | The `json_safe` function converts timestamps, numpy scalars, and missing values into strict JSON-compatible values. |
| 10. Random search experiment | Compare lookup behavior on equal queries. | A fixed seed (`42`) samples 1000 records, or all records if fewer than 1000 exist. | The same keys must be used for both trees to make the comparison fair. | One run may be unstable. | The script repeats the full query batch 5 times and reports mean and standard deviation. |
| 11. Timing measurement | Measure in-memory wall-clock search time. | The script uses `time.perf_counter()` around each repeated query batch. | It provides an observed runtime comparison under the local Python environment. | OS scheduling, cache effects, and interpreter overhead can dominate small timings. | The report labels timing as measured wall-clock execution time, not disk-level physical I/O latency. |
| 12. Tables and charts | Present results clearly. | Pandas prints tables and writes CSVs; Matplotlib saves average search time, height, total time, and node-access charts. | Tables give exact values; charts make the comparison easier to see. | Charts can become decorative if not tied to analysis. | The report explains what each chart demonstrates. |
| 13. Final interpretation | Connect implementation results to theory. | The report distinguishes theoretical properties, assumptions, observations, and limitations. | This prevents overclaiming and matches academic writing expectations. | A generic B+ Tree explanation would not satisfy the assignment. | The report starts from the transaction-indexing problem and uses the actual measured results. |

## Part 2: Complete Python Code

The complete runnable script is in:

`C:\Users\chend\OneDrive\Desktop\B+Tree\bplus_transaction_assignment.py`

The script is intentionally modular: loading/cleaning, B+ Tree nodes, insertion/search/range scan, best-fit degree estimation, JSON export, experiments, charts, and report-style interpretation are separate functions.

## Part 3: How to Run the Code

Local Windows command used for the verified run:

```powershell
& 'C:\Users\chend\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' -B bplus_transaction_assignment.py --dataset "D:\downloads\transactions_100k.csv" --output-dir outputs --sample-size 1000 --random-state 42 --repeats 5
```

Generic local command:

```powershell
python bplus_transaction_assignment.py --dataset "path\to\transactions.csv" --output-dir outputs
```

Google Colab / Jupyter:

1. Upload `bplus_transaction_assignment.py` and the dataset.
2. Install common packages if needed: `pip install pandas numpy matplotlib`.
3. Run:

```python
!python bplus_transaction_assignment.py --dataset "/content/transactions.csv" --output-dir "/content/outputs"
```

## Part 4: Expected Output Files

Generated files in `C:\Users\chend\OneDrive\Desktop\B+Tree\outputs`:

| File | Purpose |
|---|---|
| `best_fit_bplus_tree.json` | JSON export of the page-size-estimated best-fit B+ Tree. |
| `degree_4_bplus_tree.json` | JSON export of the fixed `m=4` B+ Tree. |
| `avg_search_time_comparison.png` | Bar chart comparing average lookup time. |
| `tree_height_comparison.png` | Bar chart comparing tree height. |
| `total_search_time_comparison.png` | Additional chart comparing total batch search time. |
| `avg_node_access_comparison.png` | Additional chart comparing average node accesses. |
| `best_fit_milestone_statistics.csv` | Exact milestone table. |
| `tree_statistics.csv` | Exact final tree statistics. |
| `search_comparison_results.csv` | Exact search experiment table. |
| `data_quality_audit.csv` | Data cleaning and field-conversion audit table. |
| `json_validation_results.csv` | Post-export `json.load()` validation results. |
| `search_sample_audit.csv` | Search sample size, uniqueness, and random seed audit. |

## Part 5: Report-Ready Explanation

### Abstract-Style Summary

This work implements and evaluates a B+ Tree index for transaction records with five fields: `Transaction_ID`, `Account Holder`, `Transaction_Date`, `Amount`, and `Beneficiary`. The primary index uses `Transaction_ID` as the search key, while the report also explains why date and amount range queries would normally require separate secondary B+ Tree indexes. Two trees are built from the same dataset: a page-size-based best-fit tree and a fixed-degree tree with `m = 4`. The implementation exports both trees to JSON, measures the height and width of the best-fit tree at insertion milestones, and compares lookup performance using the same 1000 randomly sampled transaction IDs.

### Data Quality Audit

| Data Audit Item | Value |
|---|---:|
| Raw rows | 100,000 |
| Rows after cleaning | 100,000 |
| Missing `Transaction_ID` rows removed | 0 |
| Duplicate `Transaction_ID` rows | 0 |
| Duplicate `Transaction_ID` unique keys | 0 |
| `Transaction_ID` normalized type | integer |
| Parsed `Transaction_Date` rows | 100,000 |
| Invalid `Transaction_Date` rows | 0 |
| Numeric `Amount` rows | 100,000 |
| Invalid `Amount` rows | 0 |

### Introduction and Motivation

The concrete problem is efficient access to transaction records. A linear scan over all transactions is simple but inefficient when the dataset grows. A B+ Tree provides a balanced multiway search structure that reduces the number of node accesses required for point lookup and supports efficient sequential access at the leaf level. In transaction systems, this is important because users may need to retrieve a unique transaction quickly by ID or scan an ordered interval of indexed keys.

### Why B+ Tree Is Suitable for Transaction Search

A B+ Tree is suitable because it is balanced and has high fanout. Theoretical property: search and insertion require `O(log_m N)` node levels, where `m` is the maximum internal-node degree. Practical implication: a larger fanout usually lowers tree height, which reduces the number of node accesses required during lookup.

### Transaction_ID as a Unique Key

`Transaction_ID` is used as the primary search key because it identifies individual transactions. The implementation treats duplicate IDs conservatively by storing duplicate records in the same leaf key bucket instead of silently dropping them. In the verified dataset, no duplicate `Transaction_ID` values were found.

### Range Queries and Linked Leaves

B+ Trees support range scans efficiently because all data records are stored in sorted leaves, and leaves are connected through `next_leaf` links. A range query first searches for the lower bound in `O(log_m N)` time and then scans `K` matching records sequentially, giving complexity `O(log_m N + K)`.

### Date and Amount Range Queries Need Secondary Indexes

The implemented tree is ordered by `Transaction_ID`. Therefore, it does not directly optimize queries such as "transactions between two dates" or "`x < Amount < y`". Those queries would normally require secondary B+ Tree indexes keyed by `Transaction_Date` or `Amount`. This distinction is important: the B+ Tree mechanism supports range queries, but only over the key order used by that particular index.

### Best-Fit Max Degree Explanation

The best-fit degree is estimated from disk-page reasoning using:

```text
max_degree = floor((page_size + key_size) / (key_size + pointer_size))
```

Using the default assumptions `page_size = 4096`, `key_size = 16`, and `pointer_size = 8`, the estimated maximum degree is `171`. Assumption: this is a page-size-based practical fanout estimate. Limitation: real databases also consider page headers, alignment, compression, variable-length keys, record layout, and buffer management.

For simplicity and consistency, this implementation uses `max_degree - 1` as the maximum number of keys for both internal and leaf nodes. In real database systems, internal-node fanout and leaf-page capacity may differ because internal pages store separator keys and child pointers, while leaf pages store record pointers or payloads.

### Best-Fit Tree vs. Degree-4 Tree

The best-fit tree represents a database-like high-fanout index. The `m=4` tree is educational and easier to visualize, but it is expected to require more levels. This expectation is confirmed by the verified run: the best-fit tree had height `3`, while the degree-4 tree had height `11`.

### Height and Width at Milestones

Verified best-fit insertion results:

| Insertion | Records | Height | Width by Level | Internal Nodes | Leaf Nodes | Total Nodes |
|---|---:|---:|---|---:|---:|---:|
| 25% | 25,000 | 3 | `[1, 2, 213]` | 3 | 213 | 216 |
| 50% | 50,000 | 3 | `[1, 4, 431]` | 5 | 431 | 436 |
| 75% | 75,000 | 3 | `[1, 4, 637]` | 5 | 637 | 642 |
| 100% | 100,000 | 3 | `[1, 8, 850]` | 9 | 850 | 859 |

Interpretation: height is the number of levels from root to leaf. Width by level reports how many nodes exist at each depth. The tree remains balanced because every leaf is at the same depth after insertion.

### JSON Export Validation

Both exported tree files were reloaded using `json.load()` after export.

| JSON File | Load Test | File Size Bytes | Tree Type | Max Degree | Total Records | Height |
|---|---|---:|---|---:|---:|---:|
| `best_fit_bplus_tree.json` | Passed | 42,823,245 | Best-fit B+ Tree | 171 | 100,000 | 3 |
| `degree_4_bplus_tree.json` | Passed | 116,639,844 | Degree-4 B+ Tree | 4 | 100,000 | 11 |

The JSON files are intentionally verbose because they store the complete tree structure and full leaf records for auditability. In a real DBMS, B+ Tree pages would normally be stored in a compact binary page format rather than JSON.

### Search Experiment Results

The experiment used the same 1000 sampled `Transaction_ID` values for both trees and repeated the batch 5 times.

| Tree | Max Degree | Height | Found | Mean Total Time (ms) | Avg Time (ms) | Avg Node Accesses | Avg Key Comparisons |
|---|---:|---:|---:|---:|---:|---:|---:|
| Best-fit B+ Tree | 171 | 3 | 1000 | 4.7888 | 0.004789 | 3.0 | 16.746 |
| Degree-4 B+ Tree | 4 | 11 | 1000 | 11.0694 | 0.011069 | 11.0 | 18.147 |

The sample audit confirms `1000` requested records, `1000` actual sampled records, `1000` unique sampled `Transaction_ID` keys, and random seed `42`. Experimental observation: the best-fit tree was faster in this run and required fewer node accesses. Theoretical explanation: higher fanout reduces height. Limitation: the measured time is in-memory Python wall-clock execution time, not disk-level physical I/O latency, and it is affected by Python overhead, OS scheduling, cache effects, and hardware.

### Complexity

| Operation | Complexity | Explanation |
|---|---|---|
| Search | `O(log_m N)` | The search follows one child per level. |
| Insert | `O(log_m N)` | The tree descends to a leaf and may split along the path to the root. |
| Range query | `O(log_m N + K)` | Find the lower bound, then scan `K` matching records across linked leaves. |
| Space | `O(N)` | Each record is stored once in a leaf bucket; internal nodes store routing keys and pointers. |

### Limitations

This implementation is insertion-only and does not implement deletion or underflow rebalancing. The best-fit degree is approximate and does not model all database page-layout costs. The same degree parameter is used for internal-node fanout and leaf-node capacity for simplicity, although real systems may tune them separately. Date and amount range queries are explained theoretically but not built as secondary indexes in the main experiment because the assignment requires storing the transaction data using `Transaction_ID` as the key. Timing measurements are local in-memory wall-clock observations rather than disk-I/O measurements or universal performance claims.

### Conclusion

The implementation satisfies the assignment by building two B+ Trees over transaction data, exporting both trees as JSON, measuring the best-fit tree shape at insertion milestones, and comparing lookup performance over the same random sample. The results demonstrate the core B+ Tree principle: a larger practical fanout can substantially reduce height and node accesses. The report should emphasize that B+ Trees support efficient range queries over their indexed key order, while date and amount ranges require separate secondary indexes to receive the same benefit.

## Part 6: Self-Check Against Assignment Requirements

| Requirement | Completed? | Evidence in Code / Report | Reviewer Risk | How We Controlled the Risk |
|---|---|---|---|---|
| Load CSV/Excel dataset | Yes | `load_transaction_dataset` supports `.csv`, `.xlsx`, `.xls`. | Excel engines may be missing in some environments. | Clear error comes from pandas; CSV verified. |
| Validate required columns | Yes | `REQUIRED_COLUMNS` check. | Column whitespace could cause false missing columns. | Column names are stripped first. |
| Clean IDs, dates, amounts | Yes | ID normalization, `pd.to_datetime`, `pd.to_numeric`. | Leading-zero IDs could be damaged. | Leading-zero IDs remain strings. |
| Handle duplicates | Yes | Duplicate IDs stored in key buckets. | Report could ignore duplicates. | Cleaning metadata reports duplicate rows and unique duplicate keys. |
| Manual B+ Tree | Yes | `BPlusTree`, `BPlusNode`, no external tree library. | Implementation bugs. | `validate()` checks sorted nodes, balanced leaves, counts, and leaf chain order. |
| Internal vs. leaf distinction | Yes | Internal `children`, leaf `records`. | Confusion with B-tree. | Report explicitly says records are only in leaves. |
| Linked leaves and range scan | Yes | `next_leaf` and `range_scan`. | Range claims may be overbroad. | Report limits range benefit to the indexed key order. |
| Best-fit degree | Yes | `estimate_best_fit_degree`, result `m=171`. | Formula may be overclaimed. | Labeled as practical estimate with assumptions and limitations. |
| Degree-4 tree | Yes | Built with `max_degree=4`. | Unrealistic fanout. | Report frames it as educational. |
| 25/50/75/100 stats | Yes | `best_fit_milestone_statistics.csv`. | Shape depends on insertion order. | Report states insertion-order dependence. |
| JSON exports | Yes | `best_fit_bplus_tree.json`, `degree_4_bplus_tree.json`, `json_validation_results.csv`. | Python pointers not serializable; large JSON files can be malformed if writing is interrupted. | Leaf links represented by node IDs, and both files were reloaded using `json.load()`. |
| 1000 random searches | Yes | Fixed seed `42`, same keys for both trees. | Dataset may have fewer than 1000 rows. | Script uses all records and prints warning. |
| Timing and comparisons | Yes | `run_search_experiment` reports time, node accesses, comparisons. | Wall-clock noise. | Uses repeated runs and reports structural metrics. |
| Bar charts | Yes | Four `.png` files generated with Matplotlib. | Charts without interpretation. | Report explains what the charts show. |
| Academic explanation | Yes | This document and script output. | Generic filler. | Explanation is tied to transaction indexing and actual results. |

## Part 7: Final Reviewer Verdict

### Strengths

The solution is assignment-aligned, runnable, and uses a real manual B+ Tree rather than a library. It separates internal routing nodes from leaf data records, supports linked-leaf range scans, handles duplicate transaction IDs conservatively, exports inspectable JSON, reload-validates both JSON files, and reports both measured wall-clock timing and structural node-access metrics. The best-fit degree is justified with a page-capacity model and not overstated as a universal optimum.

### Weaknesses

The implementation does not include deletion, redistribution after deletion, or secondary B+ Tree indexes for `Transaction_Date` and `Amount`. The best-fit degree model omits page headers, alignment, and variable-length storage. The JSON export of the degree-4 tree is large because the low fanout creates many nodes.

### What Must Be Fixed Before Submission

Nothing essential is missing for Problem 1 as stated in the assignment. Before submitting, confirm that the instructor expects only Problem 1, because the PDF also contains a separate graph-labeling Problem 2.

### What Would Make the Work Stronger

Add optional secondary indexes for `Transaction_Date` and `Amount`, then demonstrate actual range-query experiments. Include a small visualization of a tiny `m=4` tree for teaching clarity. Report results over multiple insertion orders if the assignment emphasizes tree-shape sensitivity.

### Acceptability

Current solution is acceptable for a university programming + report assignment on B+ Tree implementation. It is especially strong if the final report preserves the distinction between theoretical properties, assumptions, implementation results, experimental observations, and limitations.
