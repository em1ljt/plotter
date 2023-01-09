import os.path
import sys
import pandas as pd
import argparse

CSV_CONCAT_KEY = "CSV source"


def csv_reader(file, add_src=False, short=False):
    """
    Function to create dataframe from CSV with configured options

    :param file: filepath to CSV file to be parsed
    :param add_src: if True, add a new column with a source file identifier to the DataFrame
    :param short: (only used if add_src is True) if True, only the filename is used as the source file identifier, otherwise the entire filepath is used (default False)

    :return: pandas DataFrame
    """
    df = pd.read_csv(file, na_values=["None", "Error"], skipinitialspace=True)
    if add_src:
        file_identifier = os.path.basename(file) if short else file
        df.insert(0, CSV_CONCAT_KEY, [file_identifier for _ in range(len(df.index))])
    return df


def concat_csv(in_files, out_file=None, drop_na=False, add_src=False):
    """
    Performs an outer join on data in multiple CSV files.

    :param in_files: list of filepaths to CSV files to be merged
    :param out_file: filepath to output CSV file with merged data (default None)
    :param drop_na: if True, rows and columns with only NaN values are dropped from the dataframe (default False)
    :param add_src: if True, a new column specifying the source file for the data will be added to the datafrmae (default False)

    :return: pandas DataFrame with concatenated data
    """

    filenames = [os.path.basename(file) for file in in_files]
    short = False
    # Early return if only 1 csv file
    if len(filenames) == 1:
        df = csv_reader(in_files[0], add_src=False)
    else:
        # Check if the csv files have unique filenames
        if len(set(filenames)) == len(filenames):
            short = True

        df = csv_reader(in_files[0], add_src=add_src, short=short)

        for i in range(1, len(in_files)):

            temp_df = csv_reader(in_files[i], add_src=add_src, short=short)

            # Check if DFs have identical columns
            if len(df.columns) == len(df.columns.intersection(temp_df.columns)):
                df = pd.concat([df, temp_df], axis=0, ignore_index=True)
            else:
                return f"Unable to join CSV files: {in_files[i]} is incompatible. Can only concatenate CSVs with all columns matching."

    if drop_na:
        df.dropna(axis='columns', how='all', inplace=True)
        df.dropna(axis='rows', how='all', inplace=True)

    if out_file:
        # Output new csv file
        df.to_csv(out_file, index=False)

    return df


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(
        prog="CSV Joiner",
        description="Performs an outer join on CSV files to create a single CSV file"
    )
    parser.add_argument('out', help="output combined csv filepath")
    parser.add_argument('files', nargs='+', help="List of filepaths of CSV files to be joined")
    parser.add_argument('-d', '--dropna',
                        help='drop columns and rows with only NA values',
                        action='store_true', dest='dropna')
    parser.add_argument('-a', '--addsrc',
                        help='add a column specifying the source file for data',
                        action='store_true', dest='addsrc')
    args = parser.parse_args()
    out = args.out
    files = args.files
    dropna = args.dropna
    addsrc = args.addsrc

    # Merge files
    result = concat_csv(out_file=out, in_files=files, drop_na=dropna, add_src=addsrc)
    if isinstance(result, str):
        sys.exit(result)
