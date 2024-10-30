import setuptools

REQUIRED_PACKAGES = [
    "apache-beam[gcp]",  # Must match the version in `Dockerfile``.
    ]


setuptools.setup(
    name='tester',
    version='0.0.1',
    description='Shres Runner Package.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
    )