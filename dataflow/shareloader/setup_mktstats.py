
import setuptools

# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
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
    'openpyxl',
    'openbb-yfinance'
    ]


setuptools.setup(
    name='modules',
    version='0.0.1',
    description='Modules MkrStats Runner Package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
    )
