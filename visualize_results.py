"""Reads the pipeline's processed CSVs and plots two sales charts.

Run: python visualize_results.py
Output: sales_by_category.png, sales_by_state.png (saved in this folder)
"""
import glob
import os

import pandas as pd
import matplotlib.pyplot as plt

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "data", "processed")


def load_spark_csv(folder_name):
    """Spark writes each result as part-00000-<random-id>.csv inside a folder.
    glob finds that file without needing to know the random id in advance.
    """
    folder = os.path.join(PROCESSED_DIR, folder_name)
    csv_path = glob.glob(os.path.join(folder, "part-*.csv"))[0]
    return pd.read_csv(csv_path)


def plot_bar(df, x_col, y_col, title, output_file):
    df = df.sort_values(y_col, ascending=True)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.barh(df[x_col], df[y_col], color="#2a78d6")
    ax.set_title(title)
    ax.set_xlabel(y_col.replace("_", " ").title())
    fig.tight_layout()
    fig.savefig(output_file, dpi=150)
    plt.close(fig)
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    category_df = load_spark_csv("sales_by_category")
    plot_bar(
        category_df,
        x_col="category",
        y_col="total_revenue",
        title="Revenue by Product Category",
        output_file=os.path.join(os.path.dirname(__file__), "sales_by_category.png"),
    )

    state_df = load_spark_csv("sales_by_state")
    plot_bar(
        state_df,
        x_col="state",
        y_col="total_revenue",
        title="Revenue by Customer State",
        output_file=os.path.join(os.path.dirname(__file__), "sales_by_state.png"),
    )
