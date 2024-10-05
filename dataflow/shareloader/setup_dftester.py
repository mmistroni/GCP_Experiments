
import setuptools

# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
    "apache-beam[gcp]==2.57.0",  # Must match the version in `Dockerfile``.
    'sendgrid',
    'pandas_datareader',
    'vaderSentiment',
    'numpy',
    'bs4',
    'lxml',
    'pandas_datareader',
    'beautifulsoup4',
    'xlrd',
    'openpyxl',
    'openbb-yfinance'
    ]


setuptools.setup(
    name='shareloader',
    version='0.0.1',
    description='Shres Runner Package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
    )
