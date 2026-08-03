from __future__ import annotations

import csv
from pathlib import Path


def write_empty_output(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "product_id", "quantity", "price", "order_date", "product_name", "category"])


def run_pipeline() -> Path:
    base_dir = Path(__file__).resolve().parents[1]
    raw_dir = base_dir / "Data" / "raw"
    output_path = base_dir / "Data" / "processed" / "curated_retail_data.csv"

    retail_files = [raw_dir / "retail_data1.xlsx", raw_dir / "retail_data2.xlsx"]
    product_file = raw_dir / "product_details.xlsx"

    try:
        import pandas as pd  # type: ignore
    except ImportError:
        write_empty_output(output_path)
        return output_path

    frames = []
    for file_path in retail_files:
        if file_path.exists():
            try:
                df = pd.read_excel(file_path)
                if not df.empty:
                    frames.append(df)
            except Exception:
                continue

    if not frames:
        write_empty_output(output_path)
        return output_path

    retail_df = pd.concat(frames, ignore_index=True)

    if product_file.exists():
        try:
            product_df = pd.read_excel(product_file)
            if "product_id" in retail_df.columns and "product_id" in product_df.columns:
                retail_df = retail_df.merge(product_df, on="product_id", how="left")
        except Exception:
            pass

    retail_df = retail_df.drop_duplicates().reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    retail_df.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    curated_file = run_pipeline()
    print(f"Curated retail data written to: {curated_file}")
