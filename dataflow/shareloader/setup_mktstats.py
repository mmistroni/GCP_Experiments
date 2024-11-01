
from setuptools import setup, find_packages


# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
    "apache-beam[gcp]",  # Must match the version in `Dockerfile``.
]

setup(
    name="mypackage",  # Replace with your desired package name
    version="0.1.0",  # Replace with your desired version
    description="A short description of your package",
    author="Your Name",
    author_email="your_email@example.com",
    packages=find_packages(where="src"),
    # Look for packages under src
    package_dir={"": "src"},  # Specify the base directory for packages
    install_requires=REQUIRED_PACKAGES

)

