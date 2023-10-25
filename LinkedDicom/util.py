import os


def save_list(my_list, path):
    with open(path, "w") as file:
        for item in my_list:
            file.write("%s\n" % item)


def read_list(path):
    if not check_if_exist(path):
        return None
    else:
        with open(path, "r") as file:
            relist = file.readlines()

        # Removing newline characters from each item
        relist = [item.strip() for item in relist]
        return relist


def check_if_exist(path):
    return os.path.exists(path)


# Reading the list back from the plain text file
