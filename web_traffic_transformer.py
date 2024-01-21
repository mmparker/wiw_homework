

import polars as pl
import os

def listFilesInBucket(bucketURL, fileExtension):
    """List all files found with a matching extension at bucketURL

    In real life, I'd have permissions to list and recurse the bucket's objects; this is just a placeholder.
    """
    filepaths = [ bucketURL + letter + fileExtension for letter in "abcdefghijklmnopqrstuvwxyz" ]
    return filepaths

def downloadFilesFromS3(fileList, fileExtension):
    """Download files from S3 and convert into a list of polars DataFrames.
    
    Returns a dictionary of polars DataFrames, keyed by their filepath for easy troubleshooting.
    There's nothing S3-specific in here, actually, so could maybe be renamed...
    """
    match fileExtension:
        case ".csv":
            dfDict = { path: pl.read_csv(path) for path in fileList }
        case ".parquet":
            dfDict = { path: pl.read_parquet(path) for path in fileList }
        case ".json":
            dfDict = { path: pl.read_json(path) for path in fileList }
    return dfDict

def validateWebTrafficData(df, minimumRecords = 1, expectedColumns = ['drop', 'length', 'path', 'user_agent', 'user_id']):
    """Check if this DF has enough rows and the columns required of our web traffic data.

    Validating each file separately makes it easier to identify individual files with problems.
    """
    assert df.shape[0] >= minimumRecords, "DataFrame had {} rows, but you've specified a minimum of {}".format(df.shape[0], minimumRecords)
    assert set(df.columns) == set(expectedColumns), "DataFrame has columns {}, but you expected {}".format(df.columns, expectedColumns)
    pass


def loadWebTrafficData(bucketURL, fileExtension):
    """Read, validate, and combine the web traffic data"""
    fileList = listFilesInBucket(bucketURL, fileExtension)
    dfDict = downloadFilesFromS3(fileList, fileExtension)
    { filename: validateWebTrafficData(df) for filename, df in dfDict.items() }
    combinedDF = pl.concat([df for filename, df in dfDict.items()])
    return combinedDF


def calcTimeOnPath(df):
    """Calculate the total time each user spent on each path."""
    resultsDF = (
        df
        .group_by("user_id", "path")
        .agg(pl.sum("length").alias("total_seconds"))
        .pivot(values = "total_seconds", index = "user_id", columns = "path", aggregate_function = "sum")
    )

    return resultsDF


if __name__ == "__main__":
    webTrafficData = loadWebTrafficData("https://public.wiwdata.com/engineering-challenge/data/", ".csv")
    userTimeOnPath = calcTimeOnPath(webTrafficData)
    userTimeOnPath.write_csv("user_time_on_path.csv")
