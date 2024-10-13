
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

PROJECT_NAME = 'your_project_name'  # Replace with your project name
VERSION = '0.0.1'

# Find packages automatically
PACKAGES = setuptools.find_packages(where='modules')

# No external dependencies in this example (adjust if needed)

setuptools.setup(
    name=PROJECT_NAME,
    version=VERSION,
    packages=PACKAGES,
    install_requires=REQUIRED_PACKAGES,
)
