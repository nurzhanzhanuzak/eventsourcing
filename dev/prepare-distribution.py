# import os
# import subprocess
# import sys
# from subprocess import CalledProcessError
# from time import sleep
# 
# 
# 
# def main() -> None:
#     # Validate current working dir (should be project root).
#     proj_path = os.path.abspath(".")
#     readme_path = os.path.join(proj_path, "README.md")
#     if os.path.exists(readme_path):
#         assert "A library for event sourcing in Python" in open(readme_path).read()
#     else:
#         raise Exception("Couldn't find project README.md")
# 
#     try:
#         del os.environ["PYTHONPATH"]
#     except KeyError:
#         pass
# 
#     # # Build and upload to Test PyPI.
#     # NB: Don't upload to Test PyPI because there is a dodgy
#     # Django distribution, and there may be others:
#     # https://test.pypi.org/project/Django/3.1.10.17/
# 
#     # Build distribution.
#     subprocess.check_call([sys.executable, "setup.py", "clean", "--all"], cwd=proj_path)
#     try:
#         subprocess.check_call(
#             # [sys.executable, "setup.py", "sdist", "upload", "-r", "pypitest"],
#             [sys.executable, "setup.py", "sdist", "bdist_wheel"],
#             cwd=proj_path,
#         )
#     except CalledProcessError:
#         sys.exit(1)
# 
#     # Construct the path to the source and built distribution.
#     version_path = os.path.join(proj_path, "eventsourcing", "__init__.py")
#     version = open(version_path).readlines()[0].split("=")[-1].strip().strip('"')
#     sdist_path = os.path.join(
#         proj_path, 'dist', f'eventsourcing-{version}.tar.gz'
#     )
#     bdist_path = os.path.join(
#         proj_path, 'dist', f'eventsourcing-{version}-py3-none-any.whl'
#     )
# 
#     # Define the test targets.
#     targets = [
#         (os.path.join(proj_path, "tmpve3.7"), "python3")
#     ]
#     os.environ["CASS_DRIVER_NO_CYTHON"] = "1"
# 
#     def install_dist(dist_path, venv_path, base_python_path):
#         # Remove existing virtualenv.
#         if os.path.exists(venv_path):
#             remove_virtualenv(proj_path, venv_path)
# 
#         # Create virtualenv.
#         subprocess.check_call(
#             [base_python_path, "-m", "venv", venv_path], cwd=proj_path
#         )
# 
#         pip_path = os.path.join(venv_path, 'bin', 'pip')
#         python_path = os.path.join(venv_path, 'bin', 'python')
# 
#         subprocess.check_call(
#             [pip_path, "install", "-U", "pip", "wheel"], cwd=venv_path
#         )
# 
#         # Install from distribution.
#         pip_install_cmd = [
#             pip_path,
#             "install",
#             "--no-cache-dir",
#             f"{dist_path}[postgres_dev,crypto]",
#         ]
# 
#         patience = 10
#         is_test_pass = False
#         sleep(1)
#         while True:
#             try:
#                 subprocess.check_call(pip_install_cmd, cwd=venv_path)
#                 is_test_pass = True
#                 break
#             except CalledProcessError:
#                 patience -= 1
#                 if patience < 0:
#                     break
#                 print("Patience:", patience)
#                 sleep(1)
#         if not is_test_pass:
#             print("Failed to install.")
#             sys.exit(1)
# 
#         # # Check installed tests all pass.
#         # subprocess.check_call(
#         #     [python_path, "-m" "unittest", "discover",
#         #      "eventsourcing.tests"],
#         #     cwd=venv_path,
#         # )
# 
#         remove_virtualenv(proj_path, venv_path)
# 
#     # Test source distribution for various targets.
#     for dist_path in [sdist_path, bdist_path]:
#         for (venv_path, base_python_path) in targets:
#             install_dist(dist_path, venv_path, base_python_path)
# 
# 
# def remove_virtualenv(proj_path, venv_path):
#     subprocess.check_call(["rm", "-r", venv_path], cwd=proj_path)
# 
# 
# if __name__ == "__main__":
#     main()
