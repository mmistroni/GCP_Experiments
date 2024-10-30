from setuptools import setup, find_packages
setup(
    name="mypackage",
    version="0.1.0",  # Replace with your desired version
    author="Your Name",
    author_email="your_email@example.com",
    description="A brief description of your package",
    long_description="long_description",
    long_description_content_type="text/markdown",
    url="https://github.com/your_username/mypackage",
    packages=find_packages(where="src"),  # Specify the source directory
    package_dir={"": "src"},
    install_requires=[
        "apache-beam[gcp]",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6"

)