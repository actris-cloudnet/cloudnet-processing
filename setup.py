from setuptools import find_packages, setup

version = {}
with open("src/data_processing/version.py") as f:
    exec(f.read(), version)

setup(
    name="data_processing",
    description="Software for running Cloudnet processing.",
    version=version["__version__"],
    author="Finnish Meteorological Institute",
    license="MIT License",
    install_requires=[
        "cloudnetpy_qc>=1.8.1",
        "cloudnetpy[extras]>=1.46.4",
        "rpgpy>=0.13.1",
        "halo-reader==0.0.9",
        "pytest",
        "pylint",
        "mypy",
        "requests",
        "requests_mock",
        "types-requests",
        "types-urllib3",
        "types-toml",
        "toml",
        "influxdb-client",
        "cftime",
        "flake8",
        "black",
        "docformatter",
        "pre-commit",
    ],
    include_package_data=True,
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Development Status :: 5 - Production/Stable",
    ],
)
