import sys
import os
import subprocess

def get_sys_args(index=1):
    config_name = sys.argv[index]
    return config_name

# global
name_content_map = {}


def get_all_listdir(base_path, sub_list_dir:list, name_content_map, project_past_to, project_name):

    for dir_name in sub_list_dir:
        new_path = os.path.join(base_path, dir_name)
        create_path = os.path.join(project_past_to, dir_name)

        # print("Absolute ::: ", os.path.abspath(new_path))
        # print("create_path Absolute ::: ", os.path.abspath(create_path))
        # print("Is File ::: ", os.path.isfile(new_path))

        if os.path.isfile(new_path):
            with open(new_path, 'r') as file:
                content = file.read()

            name_content_map[create_path] = content

        else:
            list_dir = os.listdir(new_path)
            if list_dir:
                get_all_listdir(new_path, list_dir, name_content_map, create_path, project_name)

    return name_content_map

def start_read_project(project_name, project_demo_path, project_past_to):

    name_content_map = {}
    sub_list_dir = os.listdir(project_demo_path)

    if sub_list_dir:
        name_content_map = get_all_listdir(project_demo_path, sub_list_dir, name_content_map, project_past_to, project_name)

    start_process_create_dynamic_project(project_name, name_content_map)

def start_process_create_dynamic_project(project_name, name_content_map:dict):

    for src_path, content in name_content_map.items():

        if str(content).__contains__('PROJECT_NAME'):
            content = content.replace('PROJECT_NAME', project_name)

        try:
            os.makedirs(os.path.dirname(src_path), exist_ok=True)

            with open(src_path, 'w') as f:
                f.write(content)

        except Exception as e:
            print("ee",e)

def get_project_name(project_copy_from, project_past_to, project_folder_name):

    start_read_project(project_folder_name, project_copy_from, project_past_to)


if __name__ == "__main__":

    project_folder_name = get_sys_args(1)

    project_copy_from = "/home/stanley/project-folder-name"
    project_past_to = "/home/stanley/project-folder-name/{}".format(project_folder_name)

    get_project_name(project_copy_from, project_past_to, project_folder_name)