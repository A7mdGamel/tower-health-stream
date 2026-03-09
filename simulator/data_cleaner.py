import pandas as pd
import os

# Operator mapping
OPERATORS = {
    1: "Orange",
    2: "Vodafone",
    3: "e&",
    4: "WE"
}

def clean_towers(input_path: str, output_path: str):
    print(f"Loading data from {input_path}...")
    
    df = pd.read_csv(input_path, header=None, names=[
        "radio", "mcc", "net", "area", "cell", "unit",
        "lon", "lat", "range", "samples", "changeable",
        "created", "updated", "averageSignal"
    ])
    
    print(f"Total towers before cleaning: {len(df)}")

    # Remove unknown operators
    df = df[df["net"].isin(OPERATORS.keys())]
    print(f"After removing unknown operators: {len(df)}")

    # Remove missing coordinates
    df = df.dropna(subset=["lat", "lon"])
    df = df[(df["lat"] != 0) & (df["lon"] != 0)]
    print(f"After removing missing coordinates: {len(df)}")

    # Remove zero range
    df = df[df["range"] > 0]
    print(f"After removing zero range: {len(df)}")

    # Add operator name
    df["operator"] = df["net"].map(OPERATORS)

    # Keep only needed columns
    df = df[[
        "cell", "radio", "operator", "net",
        "lat", "lon", "range", "area"
    ]].rename(columns={"cell": "tower_id"})

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\nClean data saved to {output_path}")
    print(f"Final tower count: {len(df)}")
    print(f"\nOperator distribution:")
    print(df["operator"].value_counts())

if __name__ == "__main__":
    clean_towers(
        input_path=os.path.expanduser("~/Downloads/602.csv"),
        output_path="data/towers_clean.csv"
    )