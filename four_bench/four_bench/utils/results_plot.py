import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def results_plot(source_file: str, destination_file: str, **kwargs) -> None:
    """Plot results from csv file

    Args:
        source_file (str): csv file with results
        destination_file (str): destination image file
    """
    df = pd.read_csv(source_file)
    pivoted_df = df.pivot(
        index="num_of_query", columns="db", values="result"
    ).reset_index()
    melted_df = pivoted_df.melt(
        id_vars="num_of_query", var_name="Database", value_name="Speedup"
    )
    figsize = kwargs.get("figsize", (10, 6))

    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    bar_plot = sns.barplot(
        x="num_of_query", y="Speedup", hue="Database", data=melted_df, ax=ax
    )

    for patch in bar_plot.patches:
        if patch.get_height() > 0:
            bar_plot.annotate(
                f"{patch.get_height():.1f}",
                (patch.get_x() + patch.get_width() / 2.0, patch.get_height()),
                ha="center",
                va="center",
                xytext=(0, 9),
                textcoords="offset points",
            )

    plt.xlabel("Query Number")
    plt.ylabel("Speedup")
    plt.savefig(destination_file, bbox_inches="tight")
    plt.close()
