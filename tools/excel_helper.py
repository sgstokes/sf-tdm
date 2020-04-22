import os

import pandas as pd


def autosize_excel_columns(worksheet, df):
    autosize_excel_columns_df(worksheet, df.index.to_frame())
    autosize_excel_columns_df(worksheet, df, offset=df.index.nlevels)


def autosize_excel_columns_df(worksheet, df, offset=0):
    for idx, col in enumerate(df):
        series = df[col]

        max_len = max(
            series.astype(str).map(len).max(),
            max(get_length(series.name))
        )
        worksheet.set_column(idx+offset, idx+offset, max_len)


def get_length(obj):
    if isinstance(obj, tuple):
        length = [len(str(itm)) for itm in obj]
    else:
        length = [len(str(obj))]
    return length


# Write to Excel file
def write_excel(df_list, outpath, sheet_name=['Data']):

    writer = pd.ExcelWriter(path=outpath,
                            engine='xlsxwriter',
                            datetime_format='m-d-yyyy',
                            date_format='m-d-yyyy')
    # pylint: disable=abstract-class-instantiated

    for i in range(len(df_list)):
        df_list[i].to_excel(writer, sheet_name=sheet_name[i], index=True)

        worksheet = writer.sheets[sheet_name[i]]
        print(sheet_name[i])
        # Resize columns
        autosize_excel_columns(worksheet, df_list[i])
        # Data row/column start
        first_row = df_list[i].columns.nlevels
        first_column = df_list[i].index.nlevels
        # Set starting cell and format range variables
        rows = len(df_list[i])
        columns = len(df_list[i].columns)
        last_row = first_row + rows
        last_column = first_column + columns
        # Turn on autofilter
        worksheet.autofilter(first_row-1, 0, last_row-1, last_column-1)
        # Set freeze pane
        worksheet.freeze_panes(
            df_list[i].columns.nlevels, df_list[i].index.nlevels)

    writer.save()


def do_excel_out(path, results):
    df = pd.DataFrame(results)
    write_excel(df_list=[df],
                outpath=path,
                sheet_name=['Data'])
