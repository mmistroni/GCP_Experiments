from setuptools import setup, find_packages


REQUIRED_PACKAGES = [
    "apache-beam[gcp]",  # Must match the version in `Dockerfile``.
    ]


setup(
    name="mypackage",  # Replace with your desired package name
    version="0.1.0",  # Replace with your desired version
    description="A short description of your package",
    author="Your Name",
    author_email="your_email@example.com",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages()


)