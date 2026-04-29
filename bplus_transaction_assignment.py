"""
B+ Tree assignment solution for transaction records.

This script:
1. Loads and cleans a CSV/XLSX/XLS transaction dataset.
2. Builds two manual B+ Trees using Transaction_ID:
   - a page-size-based "best-fit" degree estimate
   - a fixed max degree m = 4
3. Reports best-fit tree shape after 25%, 50%, 75%, and 100% insertion.
4. Exports both trees to JSON.
5. Searches the same reproducible random sample in both trees.
6. Prints comparison tables and saves bar charts.

No external B+ Tree library is used.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Keep Matplotlib cache inside the assignment workspace. This avoids permission
# problems on managed Windows/OneDrive user directories. Stale lock files can be
# left behind after an interrupted run, so only this script's cache locks are
# cleaned before Matplotlib starts.
MPL_CACHE_DIR = Path.cwd() / ".matplotlib_cache"
MPL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
for stale_lock in MPL_CACHE_DIR.glob("*.matplotlib-lock"):
    try:
        stale_lock.unlink()
    except OSError:
        pass
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE_DIR))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REQUIRED_COLUMNS = [
    "Transaction_ID",
    "Account Holder",
    "Transaction_Date",
    "Amount",
    "Beneficiary",
]


# ---------------------------------------------------------------------------
# Data loading and cleaning
# ---------------------------------------------------------------------------


def json_safe(value: Any) -> Any:
    """Convert common pandas/numpy/datetime values into strict JSON values."""
    if value is None:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if np.isnan(value):
            return None
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    return value


def normalize_transaction_ids(series: pd.Series) -> Tuple[pd.Series, str, List[str]]:
    """Normalize Transaction_ID as int when safe, otherwise as stripped string."""
    warnings: List[str] = []
    text = series.astype("string").str.strip()
    text = text.replace({"": pd.NA})

    has_leading_zero = text.dropna().str.match(r"^0\d+$").any()
    numeric = pd.to_numeric(text, errors="coerce")
    numeric_is_complete = numeric.notna().all()

    if numeric_is_complete and not has_leading_zero:
        as_float = numeric.astype(float)
        integer_like = np.isclose(as_float % 1, 0).all()
        if integer_like:
            return numeric.astype("int64").map(int), "integer", warnings

    if has_leading_zero:
        warnings.append(
            "Transaction_ID contains leading zeros, so IDs were kept as strings "
            "to preserve identifier semantics."
        )
    else:
        warnings.append(
            "Transaction_ID could not be safely converted to integer for every row, "
            "so IDs were kept as strings."
        )
    return text.astype(str), "string", warnings


def load_transaction_dataset(path: str | Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load CSV/XLSX/XLS data and perform assignment-required validation."""
    dataset_path = Path(path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {dataset_path}")

    suffix = dataset_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(dataset_path)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(dataset_path)
    else:
        raise ValueError(
            f"Unsupported dataset extension {suffix!r}. Use .csv, .xlsx, or .xls."
        )

    original_rows = len(df)
    df.columns = [str(col).strip() for col in df.columns]

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(
            "Dataset is missing required columns: " + ", ".join(missing_columns)
        )

    df = df[REQUIRED_COLUMNS].copy()

    id_before = len(df)
    df["Transaction_ID"] = df["Transaction_ID"].astype("string").str.strip()
    missing_id_mask = df["Transaction_ID"].isna() | (df["Transaction_ID"] == "")
    missing_transaction_ids = int(missing_id_mask.sum())
    if missing_transaction_ids:
        df = df.loc[~missing_id_mask].copy()

    df["Transaction_ID"], id_type, id_warnings = normalize_transaction_ids(
        df["Transaction_ID"]
    )

    duplicate_rows = int(df.duplicated(subset=["Transaction_ID"], keep=False).sum())
    duplicate_keys = int(df["Transaction_ID"].duplicated(keep=False).nunique())
    duplicate_key_count = int(df["Transaction_ID"].duplicated(keep=False).sum())
    if duplicate_key_count:
        duplicate_unique_keys = int(
            df.loc[df["Transaction_ID"].duplicated(keep=False), "Transaction_ID"]
            .drop_duplicates()
            .shape[0]
        )
    else:
        duplicate_unique_keys = 0

    for text_col in ["Account Holder", "Beneficiary"]:
        df[text_col] = df[text_col].astype("string").str.strip()
        df[text_col] = df[text_col].replace({"": pd.NA})

    df["Transaction_Date"] = pd.to_datetime(df["Transaction_Date"], errors="coerce")
    invalid_dates = int(df["Transaction_Date"].isna().sum())

    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    invalid_amounts = int(df["Amount"].isna().sum())

    metadata = {
        "dataset_path": str(dataset_path),
        "original_rows": original_rows,
        "rows_after_cleaning": len(df),
        "removed_missing_transaction_id_rows": id_before - len(df),
        "missing_transaction_ids": missing_transaction_ids,
        "transaction_id_type": id_type,
        "duplicate_rows_by_transaction_id": duplicate_rows,
        "duplicate_transaction_id_rows": duplicate_key_count,
        "duplicate_transaction_id_unique_keys": duplicate_unique_keys,
        "invalid_transaction_dates": invalid_dates,
        "invalid_amounts": invalid_amounts,
        "warnings": id_warnings,
    }
    return df.reset_index(drop=True), metadata


def dataframe_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert cleaned DataFrame rows into JSON-safe transaction dictionaries."""
    records: List[Dict[str, Any]] = []
    for raw in df.to_dict(orient="records"):
        records.append({key: json_safe(value) for key, value in raw.items()})
    return records


# ---------------------------------------------------------------------------
# B+ Tree implementation
# ---------------------------------------------------------------------------


@dataclass
class SearchMetrics:
    found: bool
    node_accesses: int = 0
    key_comparisons: int = 0
    result_count: int = 0


@dataclass
class BPlusNode:
    is_leaf: bool
    keys: List[Any] = field(default_factory=list)
    children: List["BPlusNode"] = field(default_factory=list)
    records: List[List[Dict[str, Any]]] = field(default_factory=list)
    next_leaf: Optional["BPlusNode"] = None


def lower_bound(keys: Sequence[Any], key: Any) -> Tuple[int, int]:
    """Return first index i where keys[i] >= key, plus comparison count."""
    lo = 0
    hi = len(keys)
    comparisons = 0
    while lo < hi:
        mid = (lo + hi) // 2
        comparisons += 1
        if keys[mid] < key:
            lo = mid + 1
        else:
            hi = mid
    return lo, comparisons


def upper_bound(keys: Sequence[Any], key: Any) -> Tuple[int, int]:
    """Return first index i where keys[i] > key, plus comparison count."""
    lo = 0
    hi = len(keys)
    comparisons = 0
    while lo < hi:
        mid = (lo + hi) // 2
        comparisons += 1
        if key < keys[mid]:
            hi = mid
        else:
            lo = mid + 1
    return lo, comparisons


class BPlusTree:
    """Insertion-only B+ Tree with linked leaves and duplicate-key buckets."""

    def __init__(self, max_degree: int, name: str = "B+ Tree") -> None:
        if max_degree < 3:
            raise ValueError("max_degree must be at least 3 for a valid B+ Tree.")
        self.max_degree = int(max_degree)
        self.max_keys = self.max_degree - 1
        self.root = BPlusNode(is_leaf=True)
        self.name = name
        self.record_count = 0
        self.unique_key_count = 0

    def _find_leaf(
        self, key: Any, collect_metrics: bool = False
    ) -> Tuple[BPlusNode, List[BPlusNode], SearchMetrics]:
        node = self.root
        path: List[BPlusNode] = []
        metrics = SearchMetrics(found=False)

        while True:
            metrics.node_accesses += 1
            if node.is_leaf:
                return node, path, metrics
            path.append(node)
            child_index, comparisons = upper_bound(node.keys, key)
            if collect_metrics:
                metrics.key_comparisons += comparisons
            node = node.children[child_index]

    def insert(self, key: Any, record: Dict[str, Any]) -> None:
        leaf, path, _ = self._find_leaf(key)
        insert_index, _ = lower_bound(leaf.keys, key)

        if insert_index < len(leaf.keys) and leaf.keys[insert_index] == key:
            leaf.records[insert_index].append(record)
            self.record_count += 1
            return

        leaf.keys.insert(insert_index, key)
        leaf.records.insert(insert_index, [record])
        self.record_count += 1
        self.unique_key_count += 1

        if len(leaf.keys) > self.max_keys:
            self._split_leaf(leaf, path)

    def _split_leaf(self, leaf: BPlusNode, path: List[BPlusNode]) -> None:
        split_index = math.ceil(len(leaf.keys) / 2)
        right = BPlusNode(is_leaf=True)

        right.keys = leaf.keys[split_index:]
        right.records = leaf.records[split_index:]
        leaf.keys = leaf.keys[:split_index]
        leaf.records = leaf.records[:split_index]

        right.next_leaf = leaf.next_leaf
        leaf.next_leaf = right

        promoted_key = right.keys[0]
        self._insert_into_parent(leaf, promoted_key, right, path)

    def _insert_into_parent(
        self,
        left: BPlusNode,
        promoted_key: Any,
        right: BPlusNode,
        path: List[BPlusNode],
    ) -> None:
        if not path:
            self.root = BPlusNode(
                is_leaf=False,
                keys=[promoted_key],
                children=[left, right],
            )
            return

        parent = path.pop()
        try:
            left_index = next(
                index for index, child in enumerate(parent.children) if child is left
            )
        except StopIteration as exc:
            raise RuntimeError("B+ Tree parent-child link is inconsistent.") from exc

        parent.keys.insert(left_index, promoted_key)
        parent.children.insert(left_index + 1, right)

        if len(parent.children) > self.max_degree:
            self._split_internal(parent, path)

    def _split_internal(self, node: BPlusNode, path: List[BPlusNode]) -> None:
        middle_index = len(node.keys) // 2
        promoted_key = node.keys[middle_index]

        right = BPlusNode(is_leaf=False)
        right.keys = node.keys[middle_index + 1 :]
        right.children = node.children[middle_index + 1 :]

        node.keys = node.keys[:middle_index]
        node.children = node.children[: middle_index + 1]

        self._insert_into_parent(node, promoted_key, right, path)

    def search(self, key: Any) -> Tuple[List[Dict[str, Any]], SearchMetrics]:
        leaf, _, metrics = self._find_leaf(key, collect_metrics=True)
        index, comparisons = lower_bound(leaf.keys, key)
        metrics.key_comparisons += comparisons

        if index < len(leaf.keys) and leaf.keys[index] == key:
            records = leaf.records[index]
            metrics.found = True
            metrics.result_count = len(records)
            return records, metrics

        return [], metrics

    def range_scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        inclusive: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return records whose keys are between start_key and end_key."""
        if start_key is None:
            leaf = self.first_leaf()
            index = 0
        else:
            leaf, _, _ = self._find_leaf(start_key)
            index, _ = lower_bound(leaf.keys, start_key)

        results: List[Dict[str, Any]] = []
        while leaf is not None:
            while index < len(leaf.keys):
                key = leaf.keys[index]
                if start_key is not None:
                    if inclusive and key < start_key:
                        index += 1
                        continue
                    if not inclusive and key <= start_key:
                        index += 1
                        continue
                if end_key is not None:
                    if inclusive and key > end_key:
                        return results
                    if not inclusive and key >= end_key:
                        return results
                results.extend(leaf.records[index])
                index += 1
            leaf = leaf.next_leaf
            index = 0
        return results

    def first_leaf(self) -> BPlusNode:
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        return node

    def iter_leaves(self) -> Iterable[BPlusNode]:
        leaf = self.first_leaf()
        while leaf is not None:
            yield leaf
            leaf = leaf.next_leaf

    def stats(self) -> Dict[str, Any]:
        levels: List[List[BPlusNode]] = []
        current = [self.root]
        while current:
            levels.append(current)
            next_level: List[BPlusNode] = []
            for node in current:
                if not node.is_leaf:
                    next_level.extend(node.children)
            current = next_level

        internal_nodes = sum(1 for level in levels for node in level if not node.is_leaf)
        leaf_nodes = sum(1 for level in levels for node in level if node.is_leaf)
        width_by_level = [len(level) for level in levels]

        return {
            "tree_type": self.name,
            "max_degree": self.max_degree,
            "max_keys_per_node": self.max_keys,
            "height": len(levels),
            "width_by_level": width_by_level,
            "internal_nodes": internal_nodes,
            "leaf_nodes": leaf_nodes,
            "total_nodes": internal_nodes + leaf_nodes,
            "root_keys": [json_safe(key) for key in self.root.keys],
            "total_records": self.record_count,
            "unique_keys": self.unique_key_count,
        }

    def validate(self) -> Tuple[bool, List[str]]:
        """Run structural checks useful for catching implementation mistakes."""
        errors: List[str] = []

        def walk(node: BPlusNode, depth: int, leaf_depths: List[int]) -> None:
            if node.keys != sorted(node.keys):
                errors.append(f"Node has unsorted keys at depth {depth}: {node.keys[:5]}")
            if node.is_leaf:
                if len(node.keys) != len(node.records):
                    errors.append("Leaf key/record bucket count mismatch.")
                leaf_depths.append(depth)
            else:
                if len(node.children) != len(node.keys) + 1:
                    errors.append("Internal node child count is not key count + 1.")
                if len(node.children) > self.max_degree:
                    errors.append("Internal node exceeds max_degree.")
                for child in node.children:
                    walk(child, depth + 1, leaf_depths)

        leaf_depths: List[int] = []
        walk(self.root, 1, leaf_depths)
        if len(set(leaf_depths)) > 1:
            errors.append(f"Leaves are not all at the same depth: {sorted(set(leaf_depths))}")

        previous_key = None
        leaf_record_count = 0
        unique_key_count = 0
        for leaf in self.iter_leaves():
            for key, bucket in zip(leaf.keys, leaf.records):
                if previous_key is not None and previous_key > key:
                    errors.append("Linked leaf chain is not globally sorted.")
                    break
                previous_key = key
                unique_key_count += 1
                leaf_record_count += len(bucket)

        if leaf_record_count != self.record_count:
            errors.append(
                f"Record count mismatch: tree={self.record_count}, leaves={leaf_record_count}."
            )
        if unique_key_count != self.unique_key_count:
            errors.append(
                f"Unique-key count mismatch: tree={self.unique_key_count}, leaves={unique_key_count}."
            )

        return len(errors) == 0, errors

    def to_json_dict(self) -> Dict[str, Any]:
        node_ids: Dict[int, str] = {}
        ordered_nodes: List[BPlusNode] = []
        queue = [self.root]
        while queue:
            node = queue.pop(0)
            node_ids[id(node)] = f"node_{len(ordered_nodes)}"
            ordered_nodes.append(node)
            if not node.is_leaf:
                queue.extend(node.children)

        def serialize_node(node: BPlusNode) -> Dict[str, Any]:
            base: Dict[str, Any] = {
                "node_id": node_ids[id(node)],
                "is_leaf": node.is_leaf,
                "keys": [json_safe(key) for key in node.keys],
            }
            if node.is_leaf:
                base["next_leaf_id"] = (
                    node_ids[id(node.next_leaf)] if node.next_leaf is not None else None
                )
                base["record_groups"] = [
                    {
                        "key": json_safe(key),
                        "records": [
                            {field_name: json_safe(value) for field_name, value in rec.items()}
                            for rec in bucket
                        ],
                    }
                    for key, bucket in zip(node.keys, node.records)
                ]
            else:
                base["children"] = [serialize_node(child) for child in node.children]
            return base

        return {
            "metadata": self.stats(),
            "root": serialize_node(self.root),
        }

    def export_json(self, output_path: str | Path) -> None:
        data = self.to_json_dict()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, allow_nan=False)


# ---------------------------------------------------------------------------
# Degree estimation, building, measurements, and experiments
# ---------------------------------------------------------------------------


def estimate_best_fit_degree(
    page_size: int = 4096,
    key_size: int = 16,
    pointer_size: int = 8,
    record_pointer_size: int = 8,
) -> Dict[str, Any]:
    """Estimate practical fanout from page-size capacity assumptions."""
    if min(page_size, key_size, pointer_size, record_pointer_size) <= 0:
        raise ValueError("Page/key/pointer sizes must be positive.")

    # Internal page model: children * pointer_size + (children - 1) * key_size <= page_size
    max_degree = math.floor((page_size + key_size) / (key_size + pointer_size))
    max_degree = max(3, max_degree)

    # Leaf model shown for transparency; this implementation uses max_degree - 1 keys.
    estimated_leaf_entries = math.floor(page_size / (key_size + record_pointer_size))
    return {
        "page_size": page_size,
        "key_size": key_size,
        "pointer_size": pointer_size,
        "record_pointer_size": record_pointer_size,
        "formula": "floor((page_size + key_size) / (key_size + pointer_size))",
        "best_fit_max_degree": max_degree,
        "estimated_leaf_entries": estimated_leaf_entries,
        "interpretation": (
            "This is a practical fanout estimate based on page capacity, not a "
            "universal theoretical optimum."
        ),
    }


def build_tree(
    records: Sequence[Dict[str, Any]],
    max_degree: int,
    name: str,
    milestones: Optional[Sequence[float]] = None,
) -> Tuple[BPlusTree, List[Dict[str, Any]]]:
    tree = BPlusTree(max_degree=max_degree, name=name)
    milestone_results: List[Dict[str, Any]] = []
    total = len(records)
    milestone_targets = {
        max(1, math.ceil(total * fraction)): fraction for fraction in (milestones or [])
    }

    for index, record in enumerate(records, start=1):
        tree.insert(record["Transaction_ID"], record)
        if index in milestone_targets:
            stats = tree.stats()
            stats["inserted_records"] = index
            stats["insertion_percentage"] = f"{int(milestone_targets[index] * 100)}%"
            milestone_results.append(stats)

    if milestones and total not in milestone_targets:
        stats = tree.stats()
        stats["inserted_records"] = total
        stats["insertion_percentage"] = "100%"
        if not any(row["inserted_records"] == total for row in milestone_results):
            milestone_results.append(stats)

    ok, errors = tree.validate()
    if not ok:
        raise AssertionError("B+ Tree validation failed: " + "; ".join(errors[:5]))
    return tree, milestone_results


def run_search_experiment(
    tree: BPlusTree,
    search_keys: Sequence[Any],
    repeats: int = 5,
) -> Dict[str, Any]:
    if repeats < 1:
        raise ValueError("repeats must be at least 1.")

    total_times_ms: List[float] = []
    total_found_values: List[int] = []
    total_node_accesses = 0
    total_comparisons = 0
    total_searches = len(search_keys) * repeats

    for _ in range(repeats):
        found_this_run = 0
        start = time.perf_counter()
        for key in search_keys:
            _, metrics = tree.search(key)
            found_this_run += int(metrics.found)
            total_node_accesses += metrics.node_accesses
            total_comparisons += metrics.key_comparisons
        elapsed_ms = (time.perf_counter() - start) * 1000
        total_times_ms.append(elapsed_ms)
        total_found_values.append(found_this_run)

    stats = tree.stats()
    mean_total_time_ms = float(np.mean(total_times_ms))
    return {
        "tree": tree.name,
        "max_degree": tree.max_degree,
        "height": stats["height"],
        "searched_records": len(search_keys),
        "repeats": repeats,
        "found_records": int(total_found_values[0]) if total_found_values else 0,
        "mean_total_search_time_ms": mean_total_time_ms,
        "std_total_search_time_ms": float(np.std(total_times_ms)),
        "avg_search_time_ms": mean_total_time_ms / max(1, len(search_keys)),
        "avg_node_accesses": total_node_accesses / max(1, total_searches),
        "avg_key_comparisons": total_comparisons / max(1, total_searches),
    }


def sample_search_keys(
    df: pd.DataFrame, sample_size: int = 1000, random_state: int = 42
) -> Tuple[List[Any], Optional[str]]:
    if len(df) == 0:
        raise ValueError("Cannot sample search keys from an empty dataset.")

    warning = None
    actual_size = sample_size
    if len(df) < sample_size:
        actual_size = len(df)
        warning = (
            f"Dataset has only {len(df)} records, so all records are used instead "
            f"of sampling {sample_size}."
        )

    sampled = df.sample(n=actual_size, random_state=random_state, replace=False)
    return sampled["Transaction_ID"].tolist(), warning


# ---------------------------------------------------------------------------
# Reporting and charts
# ---------------------------------------------------------------------------


def format_table(df: pd.DataFrame) -> str:
    return df.to_string(index=False)


def save_bar_chart(
    labels: Sequence[str],
    values: Sequence[float],
    title: str,
    ylabel: str,
    output_path: str | Path,
    value_format: str = "{:.4f}",
) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 5))
    bars = plt.bar(labels, values, color=["#2f6f9f", "#c65f3f"])
    plt.title(title)
    plt.ylabel(ylabel)
    plt.grid(axis="y", linestyle="--", alpha=0.35)
    for bar, value in zip(bars, values):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            value_format.format(value),
            ha="center",
            va="bottom",
            fontsize=10,
        )
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def print_best_fit_explanation(best_fit_info: Dict[str, Any]) -> None:
    print("\nBest-fit degree estimate")
    print("-" * 80)
    print(f"Page size assumption: {best_fit_info['page_size']} bytes")
    print(f"Key size assumption: {best_fit_info['key_size']} bytes")
    print(f"Pointer size assumption: {best_fit_info['pointer_size']} bytes")
    print(f"Record pointer size assumption: {best_fit_info['record_pointer_size']} bytes")
    print(f"Formula: {best_fit_info['formula']}")
    print(f"Calculated practical max degree: {best_fit_info['best_fit_max_degree']}")
    print(f"Estimated leaf entries from key+record pointer model: {best_fit_info['estimated_leaf_entries']}")
    print(best_fit_info["interpretation"])
    print(
        "Implementation note: this script uses max_degree - 1 as the maximum "
        "number of keys for both internal and leaf nodes for simplicity. Real "
        "database systems may tune internal-node fanout and leaf-page capacity "
        "separately because leaf pages store record pointers or payloads."
    )


def build_data_audit_table(clean_metadata: Dict[str, Any]) -> pd.DataFrame:
    rows_after_cleaning = int(clean_metadata["rows_after_cleaning"])
    parsed_date_rows = rows_after_cleaning - int(clean_metadata["invalid_transaction_dates"])
    numeric_amount_rows = rows_after_cleaning - int(clean_metadata["invalid_amounts"])
    audit_rows = [
        ("Raw rows", clean_metadata["original_rows"]),
        ("Rows after cleaning", rows_after_cleaning),
        (
            "Missing Transaction_ID rows removed",
            clean_metadata["removed_missing_transaction_id_rows"],
        ),
        (
            "Duplicate Transaction_ID rows",
            clean_metadata["duplicate_transaction_id_rows"],
        ),
        (
            "Duplicate Transaction_ID unique keys",
            clean_metadata["duplicate_transaction_id_unique_keys"],
        ),
        ("Transaction_ID normalized type", clean_metadata["transaction_id_type"]),
        ("Parsed Transaction_Date rows", parsed_date_rows),
        ("Invalid Transaction_Date rows", clean_metadata["invalid_transaction_dates"]),
        ("Numeric Amount rows", numeric_amount_rows),
        ("Invalid Amount rows", clean_metadata["invalid_amounts"]),
    ]
    return pd.DataFrame(audit_rows, columns=["Data Audit Item", "Value"])


def validate_json_file(path: str | Path) -> Dict[str, Any]:
    """Reload an exported tree JSON file and return metadata evidence."""
    json_path = Path(path)
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    metadata = data.get("metadata", {})
    return {
        "json_file": json_path.name,
        "load_test": "Passed",
        "file_size_bytes": json_path.stat().st_size,
        "tree_type": metadata.get("tree_type"),
        "max_degree": metadata.get("max_degree"),
        "total_records": metadata.get("total_records"),
        "height": metadata.get("height"),
    }


def print_report_style_interpretation(
    clean_metadata: Dict[str, Any],
    best_fit_info: Dict[str, Any],
    milestone_df: pd.DataFrame,
    search_df: pd.DataFrame,
) -> None:
    print("\nReport-ready interpretation")
    print("-" * 80)
    print(
        "This experiment indexes transaction records using Transaction_ID as the "
        "primary search key. The implementation stores only separator keys and "
        "child references in internal nodes, while complete transaction records "
        "are stored in the linked leaf level. This matches the B+ Tree property "
        "that all data records are reachable at the leaves and all leaves remain "
        "at the same depth."
    )
    print(
        f"The best-fit tree uses a page-size-based degree estimate of "
        f"m = {best_fit_info['best_fit_max_degree']}. This value is an assumption-"
        "driven practical fanout estimate, not a theoretical maximum. Real systems "
        "would also account for page headers, alignment, compression, variable-length "
        "keys, and buffer manager behavior."
    )
    print(
        "The degree-4 tree is intentionally small. It is useful for learning and "
        "visualization, but it is expected to have more levels and more node accesses "
        "than a high-fanout page-oriented tree."
    )
    if not milestone_df.empty:
        final_row = milestone_df.iloc[-1]
        print(
            f"At 100% insertion, the best-fit tree has height {final_row['height']} "
            f"and width by level {final_row['width_by_level']}. Height is measured "
            "as the number of levels from root to leaf; width by level reports how "
            "many nodes appear at each depth."
        )
    if not search_df.empty:
        print(
            "The search experiment uses the same sampled Transaction_ID values for "
            "both trees, so the comparison controls for query-set variation. The "
            "measured wall-clock search time is an in-memory Python execution-time "
            "measurement, not disk-level physical I/O latency. It can be affected by "
            "Python overhead, OS scheduling, CPU cache effects, and hardware "
            "conditions. Average node accesses are a more stable proxy for index "
            "traversal work."
        )
        winner = search_df.sort_values("avg_search_time_ms").iloc[0]
        print(
            f"In this run, the lower average search time is observed for "
            f"{winner['tree']} ({winner['avg_search_time_ms']:.6f} ms per lookup). "
            "This is an experimental result, not a universal guarantee."
        )
    if clean_metadata["duplicate_transaction_id_rows"]:
        print(
            "Duplicate Transaction_ID values were detected. The implementation keeps "
            "duplicate records under the same leaf key bucket instead of silently "
            "dropping them."
        )


def auto_find_dataset() -> Optional[Path]:
    candidates: List[Path] = []
    search_roots = [Path.cwd(), Path("D:/downloads")]
    patterns = ["transactions*.csv", "transactions*.xlsx", "transactions*.xls"]
    for root in search_roots:
        if not root.exists():
            continue
        for pattern in patterns:
            candidates.extend(root.glob(pattern))
    if not candidates:
        return None
    candidates.sort(key=lambda p: (p.name != "transactions_100k.csv", str(p)))
    return candidates[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and evaluate B+ Trees for transaction data."
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Path to .csv, .xlsx, or .xls transaction dataset.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs",
        help="Directory for JSON, CSV, and chart outputs.",
    )
    parser.add_argument("--sample-size", type=int, default=1000)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--page-size", type=int, default=4096)
    parser.add_argument("--key-size", type=int, default=16)
    parser.add_argument("--pointer-size", type=int, default=8)
    parser.add_argument("--record-pointer-size", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = Path(args.dataset) if args.dataset else auto_find_dataset()
    if dataset_path is None:
        raise FileNotFoundError(
            "No dataset path was provided and no transactions*.csv/xlsx/xls file "
            "was found in the current directory or D:/downloads. Use --dataset."
        )

    print("Dataset loading and cleaning")
    print("-" * 80)
    print(f"Dataset path: {dataset_path}")
    df, clean_metadata = load_transaction_dataset(dataset_path)
    records = dataframe_to_records(df)

    print(json.dumps(clean_metadata, indent=2, ensure_ascii=False, default=str))
    data_audit_df = build_data_audit_table(clean_metadata)
    print("\nData quality audit")
    print("-" * 80)
    print(format_table(data_audit_df))
    data_audit_df.to_csv(output_dir / "data_quality_audit.csv", index=False)
    if clean_metadata["warnings"]:
        print("Warnings:")
        for warning in clean_metadata["warnings"]:
            print(f"- {warning}")
    if clean_metadata["duplicate_transaction_id_rows"]:
        print(
            "Duplicate policy: duplicate Transaction_ID values are stored as "
            "multiple records under one leaf key."
        )

    best_fit_info = estimate_best_fit_degree(
        page_size=args.page_size,
        key_size=args.key_size,
        pointer_size=args.pointer_size,
        record_pointer_size=args.record_pointer_size,
    )
    print_best_fit_explanation(best_fit_info)

    print("\nBuilding best-fit B+ Tree")
    print("-" * 80)
    best_fit_tree, milestone_rows = build_tree(
        records,
        max_degree=best_fit_info["best_fit_max_degree"],
        name="Best-fit B+ Tree",
        milestones=[0.25, 0.50, 0.75, 1.00],
    )
    milestone_df = pd.DataFrame(milestone_rows)
    milestone_columns = [
        "insertion_percentage",
        "inserted_records",
        "height",
        "width_by_level",
        "internal_nodes",
        "leaf_nodes",
        "total_nodes",
    ]
    milestone_df = milestone_df[milestone_columns]
    print(format_table(milestone_df))
    milestone_df.to_csv(output_dir / "best_fit_milestone_statistics.csv", index=False)

    print("\nBuilding degree-4 B+ Tree")
    print("-" * 80)
    degree_4_tree, _ = build_tree(
        records,
        max_degree=4,
        name="Degree-4 B+ Tree",
    )

    print("\nTree statistics")
    print("-" * 80)
    stats_df = pd.DataFrame([best_fit_tree.stats(), degree_4_tree.stats()])
    stats_columns = [
        "tree_type",
        "max_degree",
        "height",
        "width_by_level",
        "internal_nodes",
        "leaf_nodes",
        "total_nodes",
        "total_records",
        "unique_keys",
    ]
    stats_df = stats_df[stats_columns]
    print(format_table(stats_df))
    stats_df.to_csv(output_dir / "tree_statistics.csv", index=False)

    print("\nExporting JSON trees")
    print("-" * 80)
    best_fit_json = output_dir / "best_fit_bplus_tree.json"
    degree_4_json = output_dir / "degree_4_bplus_tree.json"
    best_fit_tree.export_json(best_fit_json)
    degree_4_tree.export_json(degree_4_json)
    print(f"Wrote: {best_fit_json}")
    print(f"Wrote: {degree_4_json}")

    print("\nPost-export JSON validation")
    print("-" * 80)
    json_validation_df = pd.DataFrame(
        [validate_json_file(best_fit_json), validate_json_file(degree_4_json)]
    )
    print(format_table(json_validation_df))
    json_validation_df.to_csv(output_dir / "json_validation_results.csv", index=False)

    print("\nRandom search experiment")
    print("-" * 80)
    random.seed(args.random_state)
    search_keys, sample_warning = sample_search_keys(
        df,
        sample_size=args.sample_size,
        random_state=args.random_state,
    )
    if sample_warning:
        print(f"Warning: {sample_warning}")
    sample_audit_df = pd.DataFrame(
        [
            ("Requested sample size", args.sample_size),
            ("Actual sampled records", len(search_keys)),
            ("Unique sampled Transaction_ID keys", len(set(search_keys))),
            ("Random seed", args.random_state),
        ],
        columns=["Sample Audit Item", "Value"],
    )
    print("\nSearch sample audit")
    print("-" * 80)
    print(format_table(sample_audit_df))
    sample_audit_df.to_csv(output_dir / "search_sample_audit.csv", index=False)

    experiment_rows = [
        run_search_experiment(best_fit_tree, search_keys, repeats=args.repeats),
        run_search_experiment(degree_4_tree, search_keys, repeats=args.repeats),
    ]
    search_df = pd.DataFrame(experiment_rows)
    search_columns = [
        "tree",
        "max_degree",
        "height",
        "searched_records",
        "repeats",
        "found_records",
        "mean_total_search_time_ms",
        "std_total_search_time_ms",
        "avg_search_time_ms",
        "avg_node_accesses",
        "avg_key_comparisons",
    ]
    search_df = search_df[search_columns]
    print(format_table(search_df))
    search_df.to_csv(output_dir / "search_comparison_results.csv", index=False)

    print("\nGenerating charts")
    print("-" * 80)
    labels = list(search_df["tree"])
    save_bar_chart(
        labels,
        list(search_df["avg_search_time_ms"]),
        "Average Measured Wall-Clock Search Time",
        "Average in-memory lookup time (ms)",
        output_dir / "avg_search_time_comparison.png",
        value_format="{:.6f}",
    )
    save_bar_chart(
        labels,
        list(search_df["height"]),
        "Tree Height Comparison",
        "Height (levels from root to leaf)",
        output_dir / "tree_height_comparison.png",
        value_format="{:.0f}",
    )
    save_bar_chart(
        labels,
        list(search_df["mean_total_search_time_ms"]),
        "Mean Total Measured Wall-Clock Search Time",
        "Mean in-memory time for sampled searches (ms)",
        output_dir / "total_search_time_comparison.png",
        value_format="{:.4f}",
    )
    save_bar_chart(
        labels,
        list(search_df["avg_node_accesses"]),
        "Average Node Access Comparison",
        "Average node accesses per lookup",
        output_dir / "avg_node_access_comparison.png",
        value_format="{:.2f}",
    )
    print(f"Wrote: {output_dir / 'avg_search_time_comparison.png'}")
    print(f"Wrote: {output_dir / 'tree_height_comparison.png'}")
    print(f"Wrote: {output_dir / 'total_search_time_comparison.png'}")
    print(f"Wrote: {output_dir / 'avg_node_access_comparison.png'}")

    print_report_style_interpretation(
        clean_metadata=clean_metadata,
        best_fit_info=best_fit_info,
        milestone_df=milestone_df,
        search_df=search_df,
    )


if __name__ == "__main__":
    main()
