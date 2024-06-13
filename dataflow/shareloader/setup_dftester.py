
import setuptools

# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
    'openbb',
    "apache-beam[gcp]",  # Must match the version in `Dockerfile``.
    'sendgrid',
    'pandas_datareader',
    'vaderSentiment',
    'numpy',
    'bs4',
    'lxml',
    'pandas_datareader',
    'beautifulsoup4',
    'xlrd',
    'openpyxl'
    ]


setuptools.setup(
    name='modules',
    version='0.0.1',
    description='Shres Runner Package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
    )
