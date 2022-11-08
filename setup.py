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
        "cloudnetpy_qc>=1.1.1",
        "cloudnetpy>=1.39.0",
        "cloudnetme>=0.1.5",
        "pytest",
        "pylint",
        "mypy",
        "requests",
        "requests_mock",
        "types-pytz",
        "types-requests",
        "types-urllib3",
        "types-toml",
        "toml",
        "pandas",
        "influxdb-client[ciso]",
        "cftime",
        "flake8",
    ],
    include_package_data=True,
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
    ],
)
