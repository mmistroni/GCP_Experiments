import setuptools


# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.
REQUIRED_PACKAGES = [
    'numpy',
    'beautifulsoup4',
    'pandas',
    'sendgrid',
    'lxml',
    'pandas_datareader'
    ]


setuptools.setup(
    name='modules',
    version='0.0.1',
    description='Edgar workflow package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
    )