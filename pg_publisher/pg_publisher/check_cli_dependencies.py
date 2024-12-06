import os
import subprocess
import platform


def check_program_installed(program_name):
    try:
        subprocess.run(
            [program_name, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        return True
    except FileNotFoundError:
        return False


def check_program_in_path(program_name):
    path_var = os.environ.get("PATH")
    paths = path_var.split(os.pathsep)
    for path in paths:
        exe_path = os.path.join(path, program_name)
        if os.path.exists(exe_path):
            return True
    return False


def print_error(program_name):
    print(f"Error: {program_name} is not installed or not in PATH")


def run_check_dependencies():
    if not check_program_installed("psql"):
        print_error("psql")

    if not check_program_installed("pg_dump"):
        print_error("pg_dump")

    if platform.system() == "Windows":
        if not check_program_in_path("psql.exe"):
            print_error("psql.exe")
        if not check_program_in_path("pg_dump.exe"):
            print_error("pg_dump.exe")
