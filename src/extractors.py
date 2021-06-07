import zipfile


def unzip_myfitbitdata(path_src: str, path_dst: str):
    """
    Unzip MyFitbitData.zip
    :param path_src: source path
    :param path_dst: destination path
    :return:
    """

    with zipfile.ZipFile(path_src, "r") as f:
        f.extractall(path_dst)
