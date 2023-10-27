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
        "cloudnetpy_qc>=1.10.8",
        "cloudnetpy[extras]>=1.54.1",
        "rpgpy>=0.14.2",
        "halo-reader==0.1.8",
        "requests",
        "toml",
        "influxdb-client",
        "cftime",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pylint",
            "mypy",
            "requests_mock",
            "types-requests",
            "types-urllib3",
            "types-toml",
            "flake8",
            "black",
            "docformatter",
            "pre-commit",
        ],
    },
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
