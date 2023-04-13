import os


def file_paths(res_path):
    dir_file_paths = [[f'{res_path}{filename}' for filename in filenames] for path, k, filenames in
                      os.walk(res_path)]
    all_file_paths = []

    for dir_arr in dir_file_paths:
        for file_path in dir_arr:
            all_file_paths.append(file_path)

    return all_file_paths
